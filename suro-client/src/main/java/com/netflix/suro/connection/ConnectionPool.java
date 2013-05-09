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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@LazySingleton
public class ConnectionPool {
    private static Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    private Map<Server, SuroConnection> connectionPool = new ConcurrentHashMap<Server, SuroConnection>();
    private Set<Server> serverSet = Collections.newSetFromMap(new ConcurrentHashMap<Server, Boolean>());
    private List<SuroConnection> connectionList = Collections.synchronizedList(new LinkedList<SuroConnection>());

    private final ClientConfig config;
    private final ILoadBalancer lb;

    private ScheduledExecutorService connectionSweeper;
    private ExecutorService newConnectionBuilder;
    private BlockingQueue<SuroConnection> connectionQueue = new LinkedBlockingQueue<SuroConnection>();

    @Inject
    public ConnectionPool(ClientConfig config, ILoadBalancer lb) {
        this.config = config;
        this.lb = lb;

        connectionSweeper = Executors.newScheduledThreadPool(1);
        newConnectionBuilder = Executors.newFixedThreadPool(1);

        Monitors.registerObject(this);

        populateClients();
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

    @Monitor(name = "PoolSize", type = DataSourceType.GAUGE)
    public int getPoolSize() {
        return connectionList.size();
    }

    @Monitor(name = "OutPoolSize", type = DataSourceType.GAUGE)
    private AtomicInteger outPoolSize = new AtomicInteger(0);
    public int getOutPoolSize() {
        return outPoolSize.get();
    }

    private void populateClients() {
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

    private void addConnection(Server server, SuroConnection connection, boolean inPool) {
        if (inPool) {
            connectionPool.put(server, connection);
        }
        serverSet.add(server);
        connectionList.add(connection);
    }


    private synchronized  void removeConnection(Set<Server> removedServers) {
        for (Server s : removedServers) {
            serverSet.remove(s);
            connectionPool.remove(s);
        }

        Iterator<SuroConnection> i = connectionQueue.iterator();
        while (i.hasNext()) {
            if (serverSet.contains(i.next().getServer()) == false) {
                i.remove();
                logger.info("connection was removed from the queue");
            }
        }

        i = connectionList.iterator();
        while (i.hasNext()) {
            SuroConnection c = i.next();
            if (serverSet.contains(c.getServer()) == false) {
                c.disconnect();
                i.remove();
            }
        }
    }

    public SuroConnection chooseConnection() {
        SuroConnection connection = connectionQueue.poll();
        if (connection == null) {
            connection = chooseFromPool();
        }

        for (int i = 0; i < config.getRetryCount() && connection == null; ++i) {
            Server server = lb.chooseServer(null);
            connection = new SuroConnection(server, config, false);
            try {
                connection.connect();
                outPoolSize.incrementAndGet();
                logger.info(connection + " is created out of the pool");
                break;
            } catch (Exception e) {
                logger.error("Error in connecting to " + connection + " message: " + e.getMessage(), e);
                lb.markServerDown(server);
            }
        }

        return connection;
    }

    private SuroConnection chooseFromPool() {
        SuroConnection connection = null;
        int count = 0;

        while (connection == null) {
            Server server = lb.chooseServer(null);
            if (server != null) {
                if (serverSet.contains(server) == false) {
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

    public void markServerDown(SuroConnection connection) {
        if (connection != null) {
            lb.markServerDown(connection.getServer());
            removeConnection(new ImmutableSet.Builder<Server>().add(connection.getServer()).build());
        }
    }

    private boolean shouldChangeConnection(SuroConnection connection) {
        if (connection.isInPool() == false) {
            return false;
        }

        long now = System.currentTimeMillis();

        long minimumTimeSpan = connection.getTimeUsed() + config.getMinimumReconnectTimeInterval();
        if (minimumTimeSpan <= now &&
                (connection.getSentCount() >= config.getReconnectInterval() ||
                        connection.getTimeUsed() + config.getReconnectTimeInterval() <= now)) {
            return true;
        }

        return false;
    }

    public static class SuroConnection {
        private TTransport transport;
        private SuroServer.Client client;

        private final Server server;
        private final ClientConfig config;
        private final boolean inPool;

        private int sentCount = 0;
        private long timeUsed = 0;

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
            transport.close();
        }

        public Result send(TMessageSet messageSet) throws TException {
            ++sentCount;
            if (sentCount == 1) {
                timeUsed = System.currentTimeMillis();
            }

            return client.process(messageSet);
        }

        public Server getServer() { return server; }

        public int getSentCount() {
            return sentCount;
        }

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
