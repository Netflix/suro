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

package com.netflix.suro.client;

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
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSyncSuroClient {
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
        SyncSuroClient client = injector.getInstance(SyncSuroClient.class);
        client.send(TestConnectionPool.createMessageSet(100));
        client.send(new Message("routingKey", "testMessage".getBytes()));

        assertEquals(client.getSentMessageCount(), 101);

        TestConnectionPool.checkMessageSetCount(servers, 2, false);
        TestConnectionPool.checkMessageCount(servers, 101);
    }

    @Test
    public void testRetry() throws InterruptedException {
        servers.get(0).setTryLater();

        SyncSuroClient client = injector.getInstance(SyncSuroClient.class);

        for (int i = 0; i < 10; ++i) {
            client.send(TestConnectionPool.createMessageSet(100));
        }


        TestConnectionPool.checkMessageSetCount(servers, 10, false);

        assertEquals(client.getSentMessageCount(), 1000);
        assertTrue(client.getRetriedCount() > 0);
    }

    @Test
    public void testLost() throws InterruptedException {
        SyncSuroClient client = injector.getInstance(SyncSuroClient.class);

        for (SuroServer4Test c : servers) {
            c.setTryLater();
        }

        for (int i = 0; i < 10; ++i) {
            client.send(TestConnectionPool.createMessageSet(100));
        }

        assertEquals(client.getSentMessageCount(), 0);
        assertEquals(client.getLostMessageCount(), 1000);
    }
}
