package com.netflix.suro.connection;

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

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestConnectionPool {
    private Injector injector;
    private List<SuroServer4Test> servers;
    private Properties props = new Properties();

    @Before
    public void setup() throws Exception {
        servers = startServers(3, 8300);
    }

    private void createInjector() throws Exception {
        props.put(ClientConfig.LB_SERVER, "localhost:8300,localhost:8301,localhost:8302");

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

    public static List<SuroServer4Test> startServers(int count, int startPort) throws Exception {
        List<SuroServer4Test> collectors = new LinkedList<SuroServer4Test>();
        for (int i = 0; i < count; ++i) {
            SuroServer4Test c = new SuroServer4Test(startPort + i);
            c.start();
            collectors.add(c);
        }

        return collectors;
    }

    public static void shutdownServers(List<SuroServer4Test> collectors) {
        for (SuroServer4Test c : collectors) {
            c.shutdown();
        }
    }

    public static TMessageSet createMessageSet(int messageCount) {
        MessageSetBuilder builder = new MessageSetBuilder()
                .withApp("application")
                .withDatatype("Client-" + Thread.currentThread().getId())
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
            if (c.getMessageSetCount() == 0) {
                if (unbalanceCheck) {
                    fail("unbalanced");
                }
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

        pool.markServerDown(
                new ConnectionPool.SuroConnection(
                        new Server("localhost", 8300),
                        injector.getInstance(ClientConfig.class)));
        long prevCount = servers.get(0).getMessageSetCount();

        goLatch.countDown();
        executors.awaitTermination(10, TimeUnit.SECONDS);

        int messageSetCount = 0;
        for (SuroServer4Test c : servers) {
            messageSetCount += c.getMessageSetCount();
        }
        assertEquals(messageSetCount, 10);

        assertEquals(prevCount, servers.get(0).getMessageSetCount());
    }

    @Test
    public void testReconnectInterval() throws Exception {
        props.setProperty(ClientConfig.MINIMUM_RECONNECT_TIME_INTERVAL, "1");
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
        props.setProperty(ClientConfig.MINIMUM_RECONNECT_TIME_INTERVAL, "1");
        props.setProperty(ClientConfig.RECONNECT_INTERVAL, "1000");
        props.setProperty(ClientConfig.RECONNECT_TIME_INTERVAL, "1");

        createInjector();

        ConnectionPool pool = injector.getInstance(ConnectionPool.class);

        ConnectionPool.SuroConnection connection = pool.chooseConnection();
        connection.send(TestConnectionPool.createMessageSet(100));
        pool.endConnection(connection);

        connection = pool.chooseConnection();

        assertEquals(connection.getSentCount(), 0);
    }
}
