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
import com.netflix.suro.connection.StaticLoadBalancer;
import com.netflix.suro.connection.TestConnectionPool;
import com.netflix.suro.message.Message;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAsyncSuroClient {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private Injector injector;
    private List<SuroServer4Test> servers;

    private void setupMemory(final Properties props) throws Exception {
        servers = TestConnectionPool.startServers(3, 8100);

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
        servers = TestConnectionPool.startServers(3, 8100);

        props.put(ClientConfig.LB_SERVER, "localhost:8100,localhost:8101,localhost:8102");
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
        props.put(ClientConfig.LB_SERVER, "localhost:8101,localhost:8102,localhost:8100");

        setupMemory(props);

        AsyncSuroClient client = injector.getInstance(AsyncSuroClient.class);

        for (int i = 0; i < 3000; ++i) {
            client.send(new Message("routingKey", "testMessage".getBytes()));
        }

        client.shutdown();
        TestConnectionPool.checkMessageCount(servers, 3000);

        assertEquals(client.getSentMessageCount(), 3000);
    }

    @Test
    public void testFile() throws Exception {
        setupFile(new Properties());

        AsyncSuroClient client = injector.getInstance(AsyncSuroClient.class);

        for (int i = 0; i < 3000; ++i) {
            client.send(new Message("routingKey", "testMessage".getBytes()));
        }

        client.shutdown();
        TestConnectionPool.checkMessageCount(servers, 3000);

        assertEquals(client.getSentMessageCount(), 3000);
    }

    @Test
    public void testRestore() throws Exception {
        setupFile(new Properties());

        AsyncSuroClient client = injector.getInstance(AsyncSuroClient.class);

        for (SuroServer4Test c : servers) {
            c.setTryLater();
        }

        for (int i = 0; i < 3000; ++i) {
            client.send(new Message("routingKey", "testMessage".getBytes()));
        }

        // wait until some messages are restored
        while (client.getRestoredMessageCount() < 1000) {
            Thread.sleep(10);
        }

        for (SuroServer4Test c : servers) {
            c.cancelTryLater();
        }

        // wait until alll messages are sent
        while (client.getSentMessageCount() < 3000) {
            Thread.sleep(10);
        }

        client.shutdown();
        assertEquals(client.getLostMessageCount(), 0);
        assertEquals(client.getSentMessageCount(), 3000);

        TestConnectionPool.checkMessageCount(servers, 3000);
    }

    @Test
    public void testRateLimit() throws Exception {
        Properties props = new Properties();
        props.put(AsyncSuroClient.asyncRateLimitConfig, "10");

        setupFile(props);

        AsyncSuroClient client = injector.getInstance(AsyncSuroClient.class);

        long start = System.currentTimeMillis();
        for (int i = 0; i < 50; ++i) {
            client.send(new Message("routingKey", "testMessage".getBytes()));
        }

        while (client.getSentMessageCount() < 50) {
            Thread.sleep(100);
        }

        long duration = System.currentTimeMillis() - start;
        assertTrue(duration >= 5000);
    }
}
