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

@LazySingleton
public class ConnectionPool {
    private static Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    private Map<Server, SuroConnection> connections = new ConcurrentHashMap<Server, SuroConnection>();
    private Set<Server> connectionBeingUsed = Collections.newSetFromMap(new ConcurrentHashMap<Server, Boolean>());

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
        for (Map.Entry<Server, SuroConnection> entry : connections.entrySet()) {
            entry.getValue().disconnect();
        }
        connections.clear();
        connectionBeingUsed.clear();

        connectionSweeper.shutdownNow();
        newConnectionBuilder.shutdownNow();
    }

    @Monitor(name = "PoolSize", type = DataSourceType.GAUGE)
    public int getPoolSize() {
        return connections.size();
    }

    private void populateClients() {
        for (Server server : lb.getServerList(true)) {
            SuroConnection client = new SuroConnection(server, config);
            try {
                client.connect();
                addConnection(server, client);
                logger.info(client + " is added to SuroClientPool");
            } catch (Exception e) {
                logger.error("Error in connecting to " + client + " message: " + e.getMessage(), e);
                lb.markServerDown(server);
            }
        }

        final Runnable checker = new Runnable() {
            public void run() {
                Set<Server> serversInClients = connections.keySet();
                Set<Server> serversInDiscovery = new HashSet<Server>(lb.getServerList(true));

                for (Server serverRemoved : Sets.difference(serversInClients, serversInDiscovery)) {
                    removeConnection(serverRemoved);
                    logger.info(serverRemoved + " is removed from SuroClientPool");
                }
            }
        };

        connectionSweeper.scheduleAtFixedRate(checker,
                config.getConnectionSweepInterval(),
                config.getConnectionSweepInterval(),
                TimeUnit.SECONDS);
    }

    private void addConnection(Server server, SuroConnection client) {
        connections.put(server, client);
    }

    private void removeConnection(Server server) {
        connections.remove(server).disconnect();
    }

    public SuroConnection chooseConnection() {
        SuroConnection connection = connectionQueue.poll();
        if (connection == null) {
            connection = chooseFromPool();
        }

        return connection;
    }

    private SuroConnection chooseFromPool() {
        SuroConnection connection = null;
        int count = 0;

        while (connection == null) {
            Server server = lb.chooseServer(null);
            if (server != null) {
                connection = connections.get(server);
                if (connection == null) {
                    newConnectionBuilder.execute(createNewConnection(server));
                } else if (connectionBeingUsed.contains(server)) {
                    connection = null;
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

        if (connection != null) {
            connectionBeingUsed.add(connection.getServer());
        }
        return connection;
    }

    private Runnable createNewConnection(final Server server) {
        return new Runnable() {
            @Override
            public void run() {
                if (connections.get(server) == null) {
                    SuroConnection connection = new SuroConnection(server, config);
                    try {
                        connection.connect();
                        addConnection(server, connection);
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
        if (connection != null && shouldChangeClient(connection)) {
            connection.initStat();
            connectionBeingUsed.remove(connection.getServer());
            connection = chooseFromPool();
        }

        if (connection != null) {
            connectionQueue.offer(connection);
        }
    }

    public void markServerDown(SuroConnection client) {
        Iterator<SuroConnection> i = connectionQueue.iterator();
        while (i.hasNext()) {
            if (i.next().getServer().equals(client.getServer())) {
                i.remove();
                logger.info("connection was removed from the queue");
            }
        }
        lb.markServerDown(client.getServer());
    }

    private boolean shouldChangeClient(SuroConnection connection) {
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

        private int sentCount = 0;
        private long timeUsed = 0;

        public SuroConnection(Server server, ClientConfig config) {
            this.server = server;
            this.config = config;
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

        public void initStat() {
            sentCount = 0;
            timeUsed = 0;
        }

        @Override
        public String toString() {
            return server.getHostPort();
        }
    }
}
