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

import com.google.common.base.Joiner;
import com.google.inject.Injector;
import com.netflix.governator.configuration.PropertiesConfigurationProvider;
import com.netflix.governator.guice.BootstrapBinder;
import com.netflix.governator.guice.BootstrapModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.loadbalancer.ILoadBalancer;
import com.netflix.loadbalancer.Server;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.SuroServer4Test;
import com.netflix.suro.message.Compression;
import com.netflix.suro.message.MessageSetBuilder;
import com.netflix.suro.thrift.TMessageSet;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestConnectionPool {
    private Injector injector;
    private List<SuroServer4Test> servers;
    private Properties props = new Properties();

    @Before
    public void setup() throws Exception {
        servers = startServers(3);
    }

    private void createInjector() throws Exception {
        props.put(ClientConfig.LB_SERVER, createConnectionString(servers));

        injector = LifecycleInjector.builder()
                .withBootstrapModule(new BootstrapModule() {
                    @Override
                    public void configure(BootstrapBinder binder) {
                        binder.bindConfigurationProvider().toInstance(new PropertiesConfigurationProvider(props));
                        binder.bind(ILoadBalancer.class).to(StaticLoadBalancer.class);
                    }
                }).build().createInjector();
        injector.getInstance(LifecycleManager.class).start();
    }

    @After
    public void tearDown() throws Exception {
        shutdownServers(servers);

        injector.getInstance(LifecycleManager.class).close();

        props.clear();
    }

    public static List<SuroServer4Test> startServers(int count) throws Exception {
        List<SuroServer4Test> collectors = new LinkedList<SuroServer4Test>();
        for (int i = 0; i < count; ++i) {
            SuroServer4Test c = new SuroServer4Test();
            c.start();
            collectors.add(c);
        }

        return collectors;
    }

    public static String createConnectionString(List<SuroServer4Test> servers) {
        List<String> addrList = new ArrayList<String>();
        for (SuroServer4Test c : servers) {
            addrList.add("localhost:" + c.getPort());
        }

        return Joiner.on(',').join(addrList);
    }

    public static void shutdownServers(List<SuroServer4Test> servers) {
        for (SuroServer4Test c : servers) {
            c.shutdown();
        }
    }

    public static TMessageSet createMessageSet(int messageCount) {
        MessageSetBuilder builder = new MessageSetBuilder(new ClientConfig())
                .withCompression(Compression.LZF);

        for(int i = 0; i < messageCount; ++i) {
            builder.withMessage(
                    "routingKey",
                    ("testMessage" +i).getBytes());
        }

        return builder.build();
    }

    @Test
    public void testPool() throws Exception {
        createInjector();

        final ConnectionPool pool = injector.getInstance(ConnectionPool.class);
        ExecutorService executors = Executors.newFixedThreadPool(3);
        for (int i = 0; i < 3; ++i) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 5; ++i) {
                        try {
                            ConnectionPool.SuroConnection client = pool.chooseConnection();
                            long prevTime = System.currentTimeMillis();
                            int prevCount = client.getSentCount();
                            client.send(createMessageSet(100));
                            assertEquals(client.getSentCount() - prevCount, 1);
                            if (client.getSentCount() == 1) {
                                assertTrue(
                                        client.getTimeUsed() <= System.currentTimeMillis() &&
                                                client.getTimeUsed() >= prevTime);
                            }
                            pool.endConnection(client);
                        } catch (TException e) {
                            fail(e.getMessage());
                        }
                    }
                }
            });
        }
        executors.shutdown();
        executors.awaitTermination(10, TimeUnit.SECONDS);

        checkMessageSetCount(servers, 15, true);
    }

    public static void checkMessageSetCount(List<SuroServer4Test> servers, int count, boolean unbalanceCheck) {
        int messagesetCount = 0;
        for (SuroServer4Test c : servers) {
            messagesetCount += c.getMessageSetCount();
            if (c.getMessageSetCount() == 0 && unbalanceCheck) {
                fail("unbalanced");
            }
        }
        assertEquals(messagesetCount, count);
    }

    public static void checkMessageCount(List<SuroServer4Test> servers, int count) {
        int messageCount = 0;
        for (SuroServer4Test c : servers) {
            messageCount += c.getMessageCount();
        }
        assertEquals(messageCount, count);
    }

    @Test
    public void testServerDown() throws Exception {
        props.setProperty(ClientConfig.MINIMUM_RECONNECT_TIME_INTERVAL, "0");
        props.setProperty(ClientConfig.RECONNECT_INTERVAL, "0");
        props.setProperty(ClientConfig.RECONNECT_TIME_INTERVAL, "0");

        createInjector();

        final ConnectionPool pool = injector.getInstance(ConnectionPool.class);
        final CountDownLatch waitLatch = new CountDownLatch(2);
        final CountDownLatch goLatch = new CountDownLatch(1);

        ExecutorService executors = Executors.newFixedThreadPool(2);
        for (int i = 0; i < 2; ++i) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 5; ++i) {
                        try {
                            ConnectionPool.SuroConnection client = pool.chooseConnection();
                            long prevTime = System.currentTimeMillis();
                            int prevCount = client.getSentCount();
                            client.send(createMessageSet(100));
                            assertEquals(client.getSentCount() - prevCount, 1);
                            if (client.getSentCount() == 1) {
                                assertTrue(
                                        client.getTimeUsed() <= System.currentTimeMillis() &&
                                                client.getTimeUsed() >= prevTime);
                            }
                            pool.endConnection(client);
                            if (i == 1) {
                                waitLatch.countDown();
                                goLatch.await();
                            }
                        } catch (TException e) {
                            fail(e.getMessage());
                        } catch (InterruptedException e) {
                            fail(e.getMessage());
                        }
                    }
                }
            });
        }
        executors.shutdown();
        waitLatch.await();

        Server downServer = new Server("localhost", servers.get(0).getPort());
        downServer.setAlive(true);

        ConnectionPool.SuroConnection downConnection = new ConnectionPool.SuroConnection(
                downServer,
                injector.getInstance(ClientConfig.class),
                true);
        pool.markServerDown(downConnection);
        long prevCount = servers.get(0).getMessageSetCount();

        goLatch.countDown();
        executors.awaitTermination(10, TimeUnit.SECONDS);

        int messageSetCount = 0;
        for (SuroServer4Test c : servers) {
            messageSetCount += c.getMessageSetCount();
        }
        assertEquals(messageSetCount, 10);

        assertTrue(servers.get(0).getMessageSetCount() - prevCount <= 1);
    }

    @Test
    public void testReconnectInterval() throws Exception {
        props.setProperty(ClientConfig.MINIMUM_RECONNECT_TIME_INTERVAL, "0");
        props.setProperty(ClientConfig.RECONNECT_INTERVAL, "2");
        props.setProperty(ClientConfig.RECONNECT_TIME_INTERVAL, "10000");

        createInjector();

        ConnectionPool pool = injector.getInstance(ConnectionPool.class);

        for (int i = 0; i < 2; ++i) {
            ConnectionPool.SuroConnection connection = pool.chooseConnection();
            connection.send(TestConnectionPool.createMessageSet(100));
            pool.endConnection(connection);
        }

        ConnectionPool.SuroConnection connection = pool.chooseConnection();
        assertEquals(connection.getSentCount(), 0);
    }

    @Test
    public void testReconnectTime() throws Exception {
        props.setProperty(ClientConfig.MINIMUM_RECONNECT_TIME_INTERVAL, "0");
        props.setProperty(ClientConfig.RECONNECT_INTERVAL, "1000");
        props.setProperty(ClientConfig.RECONNECT_TIME_INTERVAL, "0");

        createInjector();

        ConnectionPool pool = injector.getInstance(ConnectionPool.class);

        ConnectionPool.SuroConnection connection = pool.chooseConnection();
        connection.send(TestConnectionPool.createMessageSet(100));
        pool.endConnection(connection);

        connection = pool.chooseConnection();

        assertEquals(connection.getSentCount(), 0);
    }

    @Test
    public void shouldBePopulatedWithNumberOfServersOnLessSenderThreads() throws Exception {
        props.setProperty(ClientConfig.ASYNC_SENDER_THREADS, "1");

        createInjector();

        ILoadBalancer lb = mock(ILoadBalancer.class);
        List<Server> servers = new LinkedList<Server>();
        for (SuroServer4Test suroServer4Test : this.servers) {
            servers.add(new Server("localhost", suroServer4Test.getPort()));
        }
        when(lb.getServerList(true)).thenReturn(servers);

        ConnectionPool pool = new ConnectionPool(injector.getInstance(ClientConfig.class), lb);
        assertTrue(pool.getPoolSize() >= 1);
        for (int i = 0; i < 10; ++i) {
            if (pool.getPoolSize() != 3) {
                Thread.sleep(1000);
            }
        }
        assertEquals(pool.getPoolSize(), 3);
    }

    @Test
    public void shouldBePopulatedWithNumberOfServersOnMoreSenderThreads() throws Exception {
        props.setProperty(ClientConfig.ASYNC_SENDER_THREADS, "10");

        createInjector();

        ILoadBalancer lb = mock(ILoadBalancer.class);
        List<Server> servers = new LinkedList<Server>();
        for (SuroServer4Test suroServer4Test : this.servers) {
            servers.add(new Server("localhost", suroServer4Test.getPort()));
        }
        when(lb.getServerList(true)).thenReturn(servers);

        ConnectionPool pool = new ConnectionPool(injector.getInstance(ClientConfig.class), lb);
        assertEquals(pool.getPoolSize(), 3);
    }

    @Test
    public void shouldPopulationFinishedOnTimeout() throws Exception {
        shutdownServers(servers);

        createInjector();

        final ILoadBalancer lb = mock(ILoadBalancer.class);
        List<Server> servers = new LinkedList<Server>();
        for (SuroServer4Test suroServer4Test : this.servers) {
            servers.add(new Server("localhost", suroServer4Test.getPort()));
        }
        when(lb.getServerList(true)).thenReturn(servers);

        final AtomicBoolean passed = new AtomicBoolean(false);
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                ConnectionPool pool = new ConnectionPool(injector.getInstance(ClientConfig.class), lb);
                assertEquals(pool.getPoolSize(), 0);
                passed.set(true);
            }
        });
        t.start();
        t.join((servers.size() + 1) * injector.getInstance(ClientConfig.class).getConnectionTimeout());
        assertTrue(passed.get());
    }
}
