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

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.netflix.governator.configuration.PropertiesConfigurationProvider;
import com.netflix.governator.guice.BootstrapBinder;
import com.netflix.governator.guice.BootstrapModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.loadbalancer.ILoadBalancer;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.SuroServer4Test;
import com.netflix.suro.thrift.ResultCode;
import com.netflix.suro.thrift.TMessageSet;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Test;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestConnectionOutPool {
    private Injector injector;
    private List<SuroServer4Test> servers;
    private Properties props = new Properties();
    private ConnectionPool pool;

    @Test
    public void testOutPool() throws Exception {
        props.put(ClientConfig.ENABLE_OUTPOOL, "true");
        setup();

        ExecutorService executors = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; ++i) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 5; ++i) {
                        try {
                            ConnectionPool.SuroConnection client = pool.chooseConnection();
                            assertEquals(client.send(TestConnectionPool.createMessageSet(100)).getResultCode(), ResultCode.OK);
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

        assertTrue(pool.getOutPoolSize() > 0);
        try { Thread.sleep(1000); } catch (Exception e) { e.printStackTrace(); }
        TestConnectionPool.checkMessageSetCount(servers, 50, false);

    }

    public void setup() throws Exception {
        servers = TestConnectionPool.startServers(1);

        props.put(ClientConfig.LB_SERVER, TestConnectionPool.createConnectionString(servers));
        props.put(ClientConfig.CONNECTION_TIMEOUT, Integer.toString(Integer.MAX_VALUE));

        injector = LifecycleInjector.builder()
                .withBootstrapModule(new BootstrapModule() {
                    @Override
                    public void configure(BootstrapBinder binder) {
                        binder.bindConfigurationProvider().toInstance(new PropertiesConfigurationProvider(props));
                    }
                })
                .withAdditionalModules(new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(ILoadBalancer.class).to(StaticLoadBalancer.class);
                    }
                })
                .build().createInjector();
        injector.getInstance(LifecycleManager.class).start();

        pool = injector.getInstance(ConnectionPool.class);
        assertEquals(pool.getPoolSize(), 1);
    }

    @After
    public void after() {
        TestConnectionPool.shutdownServers(servers);
    }

    @Test
    public void shouldConnectionOutPoolStopGrowing() throws Exception {
        setup();

        for (SuroServer4Test server : servers) {
            server.setHoldConnection();
        }

        int numThreads = 10;
        final CountDownLatch latch = new CountDownLatch(numThreads);

        ExecutorService executors = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; ++i) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    TMessageSet messageSet = TestConnectionPool.createMessageSet(100);
                    boolean set = true;
                    while (true) {
                        ConnectionPool.SuroConnection connection = pool.chooseConnection();
                        if (set) {
                            latch.countDown();
                            set = false;
                        }
                        if (connection == null) {
                            continue;
                        }
                        try {
                            ResultCode result = connection.send(messageSet).getResultCode();
                            System.out.println("result code: " + result);
                            if (result == ResultCode.OK) {
                                break;
                            }
                        } catch (Exception e) {
                        } finally {
                            pool.endConnection(connection);
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    System.out.println("finished");
                }
            });
        }
        latch.await();

        assertEquals(pool.getOutPoolSize(), 0);

        for (SuroServer4Test server : servers) {
            server.cancelHoldConnection();
        }

        executors.shutdown();
        executors.awaitTermination(60, TimeUnit.SECONDS);

        try { Thread.sleep(1000); } catch (Exception e) { e.printStackTrace(); }
        TestConnectionPool.checkMessageSetCount(servers, numThreads, false);
    }
}
