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
import com.netflix.suro.message.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestAsyncSuroClient {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private Injector injector;
    private List<SuroServer4Test> servers;

    @Before
    public void setup() throws Exception {
        servers = TestConnectionPool.startServers(3);
    }

    private void setupMemory(final Properties props) throws Exception {
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

    private void setupFile(final Properties props) throws Exception {
        props.put(ClientConfig.LB_SERVER, TestConnectionPool.createConnectionString(servers));
        props.put(ClientConfig.ASYNC_FILEQUEUE_PATH, tempDir.newFolder().getAbsolutePath());
        props.put(ClientConfig.ASYNC_QUEUE_TYPE, "file");

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
    public void testMemory() throws Exception {
        Properties props = new Properties();
        props.put(ClientConfig.LB_SERVER, TestConnectionPool.createConnectionString(servers));

        setupMemory(props);

        AsyncSuroClient client = injector.getInstance(AsyncSuroClient.class);

        int messageCount = 10;
        for (int i = 0; i < messageCount; ++i) {
            client.send(new Message("routingKey", "testMessage".getBytes()));
        }

        client.shutdown();
        TestConnectionPool.checkMessageCount(servers, messageCount);

        assertEquals(client.getSentMessageCount(), messageCount);
    }

    @Test
    public void testFile() throws Exception {
        setupFile(new Properties());

        AsyncSuroClient client = injector.getInstance(AsyncSuroClient.class);

        int messageCount = 10;
        for (int i = 0; i < messageCount; ++i) {
            client.send(new Message("routingKey", "testMessage".getBytes()));
        }

        client.shutdown();
        TestConnectionPool.checkMessageCount(servers, messageCount);

        assertEquals(client.getSentMessageCount(), messageCount);
    }

    @Test
    public void testRestore() throws Exception {
        Properties props = new Properties();
        props.setProperty(ClientConfig.RETRY_COUNT, "1");
        props.setProperty(ClientConfig.ASYNC_TIMEOUT, "1");
        setupFile(props);

        int messageCount = 3;
        AsyncSuroClient client = injector.getInstance(AsyncSuroClient.class);

        final CountDownLatch restoreLatch = new CountDownLatch(messageCount / 3);
        final CountDownLatch sentLatch = new CountDownLatch(messageCount);
        client.addListener(new AsyncSuroClient.Listener() {
            @Override
            public void sentCallback(int count) {
                for (int i = 0; i < count; ++i) {
                    sentLatch.countDown();
                }
            }

            @Override
            public void restoredCallback() {
                restoreLatch.countDown();
            }

            @Override
            public void lostCallback(int count) {
                fail("should not be lost");
            }

            @Override
            public void retriedCallback() {

            }
        });

        for (SuroServer4Test c : servers) {
            c.setTryLater();
        }

        for (int i = 0; i < messageCount; ++i) {
            client.send(new Message("routingKey", "testMessage".getBytes()));
        }

        restoreLatch.await(10, TimeUnit.SECONDS);
        assertEquals(restoreLatch.getCount(), 0);

        for (SuroServer4Test c : servers) {
            c.cancelTryLater();
        }
        injector.getInstance(ConnectionPool.class).populateClients();

        sentLatch.await(60, TimeUnit.SECONDS);
        assertEquals(client.getSentMessageCount(), messageCount);
        assertEquals(client.getLostMessageCount(), 0);

        client.shutdown();

        TestConnectionPool.checkMessageCount(servers, messageCount);
    }

    @Test
    public void shouldBeBlockedOnJobQueueFull() throws Exception {
        for (SuroServer4Test c : servers) {
            c.setHoldConnection();
        }
        Properties props = new Properties();
        props.setProperty(ClientConfig.ASYNC_JOBQUEUE_CAPACITY, "1");
        props.setProperty(ClientConfig.ASYNC_SENDER_THREADS, "1");
        props.setProperty(ClientConfig.CONNECTION_TIMEOUT, Integer.toString(Integer.MAX_VALUE));

        setupFile(props);

        AsyncSuroClient client = injector.getInstance(AsyncSuroClient.class);

        for (int i = 0; i < 3000; ++i) {
            client.send(new Message("routingKey", "testMessage".getBytes()));
        }
        client.shutdown();

        assertEquals(client.queuedMessageSetCount, 2);

    }
}
