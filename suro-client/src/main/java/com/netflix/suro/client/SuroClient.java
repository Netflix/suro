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
import com.netflix.suro.client.async.AsyncSuroClient;
import com.netflix.suro.connection.ConnectionPool;
import com.netflix.suro.connection.EurekaLoadBalancer;
import com.netflix.suro.connection.StaticLoadBalancer;
import com.netflix.suro.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SuroClient implements ISuroClient {
    static Logger log = LoggerFactory.getLogger(SuroClient.class);

    private final ISuroClient client;

    public SuroClient(Properties properties) {
        createInjector(properties);
        injector.getInstance(ConnectionPool.class);
        client = injector.getInstance(ISuroClient.class);
    }

    public void shutdown() {
        injector.getInstance(LifecycleManager.class).close();
    }

    private Injector injector;

    private Injector createInjector(final Properties properties) {
        injector = LifecycleInjector
                .builder()
                .withBootstrapModule
                        (
                                new BootstrapModule() {
                                    @Override
                                    public void configure(BootstrapBinder binder) {
                                        binder.bindConfigurationProvider().toInstance(
                                                new PropertiesConfigurationProvider(properties));

                                        if (properties.getProperty(ClientConfig.LB_TYPE, "eureka").equals("eureka")) {
                                            binder.bind(ILoadBalancer.class).to(EurekaLoadBalancer.class);
                                        } else {
                                            binder.bind(ILoadBalancer.class).to(StaticLoadBalancer.class);
                                        }

                                        if (properties.getProperty(ClientConfig.CLIENT_TYPE, "async").equals("async")) {
                                            binder.bind(ISuroClient.class).to(AsyncSuroClient.class);
                                        } else {
                                            binder.bind(ISuroClient.class).to(SyncSuroClient.class);
                                        }
                                    }
                                }
                        )
                .createInjector();
        LifecycleManager manager = injector.getInstance(LifecycleManager.class);

        try {
            manager.start();
        } catch (Exception e) {
            throw new RuntimeException("LifecycleManager cannot start with an exception: " + e.getMessage());
        }
        return injector;
    }

    @Override
    public void send(Message message) {
        client.send(message);
    }

    @Override
    public long getSentMessageCount() {
        return client.getSentMessageCount();
    }

    @Override
    public long getLostMessageCount() {
        return client.getLostMessageCount();
    }

    public ClientConfig getConfig() {
        return injector.getInstance(ClientConfig.class);
    }
}
