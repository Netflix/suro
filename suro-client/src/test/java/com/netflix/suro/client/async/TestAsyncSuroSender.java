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

package com.netflix.suro.client.async;

import com.google.inject.Injector;
import com.netflix.governator.configuration.PropertiesConfigurationProvider;
import com.netflix.governator.guice.BootstrapBinder;
import com.netflix.governator.guice.BootstrapModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.loadbalancer.ILoadBalancer;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.SuroServer4Test;
import com.netflix.suro.connection.ConnectionPool;
import com.netflix.suro.connection.StaticLoadBalancer;
import com.netflix.suro.connection.TestConnectionPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAsyncSuroSender {
    private Injector injector;
    private List<SuroServer4Test> servers;

    @Before
    public void setup() throws Exception {
        servers = TestConnectionPool.startServers(3);

        final Properties props = new Properties();
        props.setProperty(ClientConfig.LB_SERVER, TestConnectionPool.createConnectionString(servers));
        props.setProperty(ClientConfig.MINIMUM_RECONNECT_TIME_INTERVAL, "1");
        props.setProperty(ClientConfig.RECONNECT_INTERVAL, "1");
        props.setProperty(ClientConfig.RECONNECT_TIME_INTERVAL, "1");
        props.setProperty(ClientConfig.APP, "app");

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
        TestConnectionPool.shutdownServers(servers);

        injector.getInstance(LifecycleManager.class).close();
    }

    @Test
    public void test() {
        AsyncSuroClient client = injector.getInstance(AsyncSuroClient.class);

        AsyncSuroSender sender = new AsyncSuroSender(
                TestConnectionPool.createMessageSet(100),
                client,
                injector.getInstance(ClientConfig.class));

        sender.run();

        assertEquals(client.getSentMessageCount(), 100);

        TestConnectionPool.checkMessageSetCount(servers, 1, false);
    }

    @Test
    public void testMultithread() throws InterruptedException {
        AsyncSuroClient client = injector.getInstance(AsyncSuroClient.class);

        ExecutorService executors = Executors.newFixedThreadPool(3);
        for (int i = 0; i < 10; ++i) {
            executors.submit(new AsyncSuroSender(
                    TestConnectionPool.createMessageSet(100),
                    client,
                    injector.getInstance(ClientConfig.class)));
        }

        executors.shutdown();
        executors.awaitTermination(10, TimeUnit.SECONDS);

        TestConnectionPool.checkMessageSetCount(servers, 10, true);

        assertEquals(client.getSentMessageCount(), 1000);
    }

    @Test
    public void testRetry() throws InterruptedException {
        AsyncSuroClient client = injector.getInstance(AsyncSuroClient.class);

        ExecutorService executors = Executors.newFixedThreadPool(3);
        for (int i = 0; i < 3; ++i) {
            executors.submit(new AsyncSuroSender(
                    TestConnectionPool.createMessageSet(100),
                    client,
                    injector.getInstance(ClientConfig.class)));
        }
        executors.shutdown();
        executors.awaitTermination(10, TimeUnit.SECONDS);
        TestConnectionPool.checkMessageSetCount(servers, 3, false);

        servers.get(0).setTryLater();

        executors = Executors.newFixedThreadPool(3);
        for (int i = 0; i < 7; ++i) {
            executors.submit(new AsyncSuroSender(
                    TestConnectionPool.createMessageSet(100),
                    client,
                    injector.getInstance(ClientConfig.class)));
        }


        executors.shutdown();
        executors.awaitTermination(10, TimeUnit.SECONDS);

        TestConnectionPool.checkMessageSetCount(servers, 10, false);

        assertEquals(client.getSentMessageCount(), 1000);
        assertTrue(client.getRetriedCount() > 0);
    }

    @Test
    public void testRestore() throws InterruptedException {
        AsyncSuroClient client = injector.getInstance(AsyncSuroClient.class);

        for (SuroServer4Test c : servers) {
            c.setTryLater();
        }

        AsyncSuroSender sender = new AsyncSuroSender(
                TestConnectionPool.createMessageSet(600),
                client,
                injector.getInstance(ClientConfig.class));

        sender.run();

        assertEquals(client.getSentMessageCount(), 0);
        assertTrue(client.getRestoredMessageCount() >= 600);
        for (SuroServer4Test c : servers) {
            c.cancelTryLater();
        }
        injector.getInstance(ConnectionPool.class).populateClients();

        // wait until client restored
        while (client.getSentMessageCount() < 600) {
            System.out.println("sent: " + client.getSentMessageCount());
            Thread.sleep(1000);
        }
        client.shutdown();
        TestConnectionPool.checkMessageCount(servers, 600);
    }
}
