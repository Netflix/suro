/*
 * Copyright 2013 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.suro.connection;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.netflix.governator.guice.lazy.LazySingleton;
import com.netflix.loadbalancer.ILoadBalancer;
import com.netflix.loadbalancer.Server;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.thrift.Result;
import com.netflix.suro.thrift.ServiceStatus;
import com.netflix.suro.thrift.SuroServer;
import com.netflix.suro.thrift.TMessageSet;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;

/**
 * Pooling for thrift connection to suro-server
 * After creating all connections to suro-server discovered by {@link ILoadBalancer}, a {@code ConnectionPool} returns
 * a connection when the client requests to get one. When there's no connection available, {@code ConnectionPool} will
 * create a new connection immediately. This is called OutPool connection.
 *
 * @author jbae
 */
@LazySingleton
public class ConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    private Map<Server, SuroConnection> connectionPool = new ConcurrentHashMap<Server, SuroConnection>();
    private Set<Server> serverSet = Collections.newSetFromMap(new ConcurrentHashMap<Server, Boolean>());
    private List<SuroConnection> connectionList = Collections.synchronizedList(new LinkedList<SuroConnection>());

    private final ClientConfig config;
    private final ILoadBalancer lb;

    private ScheduledExecutorService connectionSweeper;
    private ExecutorService newConnectionBuilder;
    private BlockingQueue<SuroConnection> connectionQueue = new LinkedBlockingQueue<SuroConnection>();
    private CountDownLatch populationLatch;

    /**
     *
     * @param config Client configuration
     * @param lb LoadBalancer implementation
     */
    @Inject
    public ConnectionPool(ClientConfig config, ILoadBalancer lb) {
        this.config = config;
        this.lb = lb;

        connectionSweeper = Executors.newScheduledThreadPool(1);
        newConnectionBuilder = Executors.newFixedThreadPool(1);

        Monitors.registerObject(this);

        populationLatch = new CountDownLatch(Math.min(lb.getServerList(true).size(), config.getAsyncSenderThreads()));
        Executors.newSingleThreadExecutor().submit(new Runnable() {
            @Override
            public void run() {
                populateClients();
            }
        });
        try {
            populationLatch.await(populationLatch.getCount() * config.getConnectionTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("Exception on CountDownLatch awaiting: " + e.getMessage(), e);
        }
        logger.info("ConnectionPool population finished with the size: " + getPoolSize()
                + ", will continue up to: " + lb.getServerList(true).size());
    }

    @PreDestroy
    public void shutdown() {
        serverSet.clear();
        connectionPool.clear();
        connectionQueue.clear();
        for (SuroConnection conn : connectionList) {
            conn.disconnect();
        }

        connectionSweeper.shutdownNow();
        newConnectionBuilder.shutdownNow();
    }

    /**
     * @return number of connections in the pool
     */
    @Monitor(name = "PoolSize", type = DataSourceType.GAUGE)
    public int getPoolSize() {
        return connectionList.size();
    }

    @Monitor(name = "OutPoolSize", type = DataSourceType.GAUGE)
    private int outPoolSize = 0;

    /**
     * @return number of connections created out of the pool
     */
    public int getOutPoolSize() {
        return outPoolSize;
    }

    public void populateClients() {
        for (Server server : lb.getServerList(true)) {
            SuroConnection connection = new SuroConnection(server, config, true);
            try {
                connection.connect();
                addConnection(server, connection, true);
                logger.info(connection + " is added to SuroClientPool");
            } catch (Exception e) {
                logger.error("Error in connecting to " + connection + " message: " + e.getMessage(), e);
                lb.markServerDown(server);
            }
        }

        connectionSweeper.scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    public void run() {
                        removeConnection(Sets.difference(serverSet, new HashSet<Server>(lb.getServerList(true))));
                    }
                },
                config.getConnectionSweepInterval(),
                config.getConnectionSweepInterval(),
                TimeUnit.SECONDS);
    }

    @VisibleForTesting
    protected void addConnection(Server server, SuroConnection connection, boolean inPool) {
        if (inPool) {
            connectionPool.put(server, connection);
            if (populationLatch.getCount() > 0) {
                populationLatch.countDown();
            }
        }
        serverSet.add(server);
        connectionList.add(connection);
    }


    private synchronized void removeConnection(Set<Server> removedServers) {
        for (Server s : removedServers) {
            serverSet.remove(s);
            connectionPool.remove(s);
        }

        Iterator<SuroConnection> i = connectionQueue.iterator();
        while (i.hasNext()) {
            if (!serverSet.contains(i.next().getServer())) {
                i.remove();
                logger.info("connection was removed from the queue");
            }
        }

        i = connectionList.iterator();
        while (i.hasNext()) {
            SuroConnection c = i.next();
            if (!serverSet.contains(c.getServer())) {
                c.disconnect();
                i.remove();
            }
        }
    }

    /**
     * When the client calls this method, it will return the connection.
     * @return connection
     */
    public SuroConnection chooseConnection() {
        SuroConnection connection = connectionQueue.poll();
        if (connection == null) {
            connection = chooseFromPool();
        }

        if (config.getEnableOutPool()) {
            synchronized (this) {
                for (int i = 0; i < config.getRetryCount() && connection == null; ++i) {
                    Server server = lb.chooseServer(null);
                    if (server != null) {
                        connection = new SuroConnection(server, config, false);
                        try {
                            connection.connect();
                            ++outPoolSize;
                            logger.info(connection + " is created out of the pool");
                            break;
                        } catch (Exception e) {
                            logger.error("Error in connecting to " + connection + " message: " + e.getMessage(), e);
                            lb.markServerDown(server);
                        }
                    }
                }
            }
        }

        if (connection == null) {
            logger.error("No valid connection exists after " + config.getRetryCount() + " retries");
        }

        return connection;
    }

    private SuroConnection chooseFromPool() {
        SuroConnection connection = null;
        int count = 0;

        while (connection == null) {
            Server server = lb.chooseServer(null);
            if (server != null) {
                if (!serverSet.contains(server)) {
                    newConnectionBuilder.execute(createNewConnection(server, true));
                } else {
                    connection = connectionPool.remove(server);
                }
            } else {
                break;
            }

            ++count;
            if (count >= 10) {
                logger.error("no connection available selected in 10 retries");
                break;
            }
        }

        return connection;
    }

    private Runnable createNewConnection(final Server server, final boolean inPool) {
        return new Runnable() {
            @Override
            public void run() {
                if (connectionPool.get(server) == null) {
                    SuroConnection connection = new SuroConnection(server, config, inPool);
                    try {
                        connection.connect();
                        addConnection(server, connection, inPool);
                        logger.info(connection + " is added to ConnectionPool");
                    } catch (Exception e) {
                        logger.error("Error in connecting to " + connection + " message: " + e.getMessage(), e);
                        lb.markServerDown(server);
                    }
                }
            }
        };
    }

    /**
     * When the client finishes communication with the client, this method
     * should be called to release the connection and return it to the pool.
     * @param connection
     */
    public void endConnection(SuroConnection connection) {
        if (connection != null && shouldChangeConnection(connection)) {
            connection.initStat();
            connectionPool.put(connection.getServer(), connection);
            connection = chooseFromPool();
        }

        if (connection != null) {
            connectionQueue.offer(connection);
        }
    }

    /**
     * Mark up the server related with the connection as down
     * When the client fails to communicate with the connection,
     * this method should be called to remove the server from the pool
     * @param connection
     */
    public void markServerDown(SuroConnection connection) {
        if (connection != null) {
            lb.markServerDown(connection.getServer());
            removeConnection(new ImmutableSet.Builder<Server>().add(connection.getServer()).build());
        }
    }

    private boolean shouldChangeConnection(SuroConnection connection) {
        if (!connection.isInPool()) {
            return false;
        }

        long now = System.currentTimeMillis();

        long minimumTimeSpan = connection.getTimeUsed() + config.getMinimumReconnectTimeInterval();
        return connectionExpired(connection, now, minimumTimeSpan);

    }

    private boolean connectionExpired(SuroConnection connection, long now, long minimumTimeSpan) {
        return minimumTimeSpan <= now &&
            (connection.getSentCount() >= config.getReconnectInterval() ||
                connection.getTimeUsed() + config.getReconnectTimeInterval() <= now);
    }

    /**
     * Thrift socket connection wrapper with configuration
     */
    public static class SuroConnection {
        private TTransport transport;
        private SuroServer.Client client;

        private final Server server;
        private final ClientConfig config;
        private final boolean inPool;

        private int sentCount = 0;
        private long timeUsed = 0;

        /**
         * @param server hostname and port information
         * @param config properties including timeout, etc
         * @param inPool whether this connection is in the pool or out of it
         */
        public SuroConnection(Server server, ClientConfig config, boolean inPool) {
            this.server = server;
            this.config = config;
            this.inPool = inPool;
        }

        public void connect() throws Exception {
            TSocket socket = new TSocket(server.getHost(), server.getPort(), config.getConnectionTimeout());
            socket.getSocket().setTcpNoDelay(true);
            socket.getSocket().setKeepAlive(true);
            socket.getSocket().setSoLinger(true, 0);
            transport = new TFramedTransport(socket);
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);

            client = new SuroServer.Client(protocol);
            ServiceStatus status = client.getStatus();
            if (status != ServiceStatus.ALIVE) {
                transport.close();
                throw new RuntimeException(server + " IS NOT ALIVE!!!");
            }
        }

        public void disconnect() {
            try {
                transport.flush();
            } catch (TTransportException e) {
                logger.error("Exception on disconnect: " + e.getMessage(), e);
            } finally {
                transport.close();
            }
        }

        public Result send(TMessageSet messageSet) throws TException {
            ++sentCount;
            if (sentCount == 1) {
                timeUsed = System.currentTimeMillis();
            }

            return client.process(messageSet);
        }

        public Server getServer() { return server; }

        /**
         * @return How many times send() method is called
         */
        public int getSentCount() {
            return sentCount;
        }

        /**
         * For the connection retention control
         * @return how long it has been used from the client
         */
        public long getTimeUsed() {
            return timeUsed;
        }

        public boolean isInPool() {
            return inPool;
        }

        public void initStat() {
            sentCount = 0;
            timeUsed = 0;
        }

        @Override
        public String toString() {
            return server.getHostPort();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Server) {
                return server.equals(o);
            } else if (o instanceof SuroConnection) {
                return server.equals(((SuroConnection) o).server);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return server.hashCode();
        }
    }
}
