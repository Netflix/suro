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

package com.netflix.suro.queue;

import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.netflix.governator.configuration.PropertiesConfigurationProvider;
import com.netflix.governator.guice.BootstrapBinder;
import com.netflix.governator.guice.BootstrapModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.suro.connection.TestConnectionPool;
import com.netflix.suro.thrift.ResultCode;
import com.netflix.suro.thrift.TMessageSet;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TestQueueManager {
    @Test
    public void test() throws Exception {
        final Properties props = new Properties();

        Injector injector = LifecycleInjector.builder()
                .withBootstrapModule(new BootstrapModule() {
                    @Override
                    public void configure(BootstrapBinder binder) {
                        binder.bindConfigurationProvider().toInstance(new PropertiesConfigurationProvider(props));
                        binder.bind(new TypeLiteral<BlockingQueue<TMessageSet>>() {})
                                .toProvider(new Provider<LinkedBlockingQueue<TMessageSet>>() {
                                    @Override
                                    public LinkedBlockingQueue<TMessageSet> get() {
                                        return new LinkedBlockingQueue<TMessageSet>(1);
                                    }
                                });

                    }
                }).build().createInjector();
        injector.getInstance(LifecycleManager.class).start();

        MessageQueue queue = injector.getInstance(MessageQueue.class);
        QueueManager manager = injector.getInstance(QueueManager.class);
        manager.registerService(queue);

        assertEquals(queue.process(TestConnectionPool.createMessageSet(100)).getResultCode(), ResultCode.OK);
        queue.poll(1, TimeUnit.SECONDS);
        assertEquals(manager.getStatus(), QueueManager.OK);

        manager.stopTakingTraffic();
        assertEquals(queue.process(TestConnectionPool.createMessageSet(100)).getResultCode(), ResultCode.OTHER_ERROR);

        manager.startTakingTraffic();
        assertEquals(queue.process(TestConnectionPool.createMessageSet(100)).getResultCode(), ResultCode.OK);
    }
}
