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

package com.netflix.suro.input.thrift;

import com.google.inject.Injector;
import com.netflix.governator.configuration.PropertiesConfigurationProvider;
import com.netflix.governator.guice.BootstrapBinder;
import com.netflix.governator.guice.BootstrapModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.suro.connection.TestConnectionPool;
import com.netflix.suro.input.thrift.MessageSetProcessor;
import com.netflix.suro.input.thrift.ServerConfig;
import com.netflix.suro.thrift.ResultCode;
import com.netflix.suro.thrift.ServiceStatus;
import com.netflix.suro.thrift.TMessageSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TestMessageSetProcessor {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private Injector injector;

    @Test
    public void testMemoryQueue() throws Exception {
        final Properties props = new Properties();

        testQueue(props);
    }

    @Test
    public void testFileQueue() throws Exception {
        final Properties props = new Properties();

        props.setProperty(ServerConfig.QUEUE_TYPE, "file");
        props.setProperty(ServerConfig.FILEQUEUE_PATH, tempDir.newFolder().getAbsolutePath());

        testQueue(props);
    }

    private void testQueue(final Properties props) throws Exception {
        injector = LifecycleInjector.builder()
                .withBootstrapModule(new BootstrapModule() {
                    @Override
                    public void configure(BootstrapBinder binder) {
                        binder.bindConfigurationProvider().toInstance(new PropertiesConfigurationProvider(props));
                    }
                }).build().createInjector();
        injector.getInstance(LifecycleManager.class).start();

        MessageSetProcessor queue = injector.getInstance(MessageSetProcessor.class);

        assertEquals(queue.getQueueSize(), 0);
        assertEquals(queue.getStatus(), ServiceStatus.ALIVE);

        TMessageSet messageSet = TestConnectionPool.createMessageSet(100);
        assertEquals(queue.process(messageSet).getResultCode(), ResultCode.OK);

        assertEquals(queue.getQueueSize(), 1);
        assertEquals(queue.poll(1, TimeUnit.MILLISECONDS), messageSet);
        assertEquals(queue.getQueueSize(), 0);

        queue.stopTakingTraffic();
        assertEquals(queue.process(messageSet).getResultCode(), ResultCode.OTHER_ERROR);

        queue.startTakingTraffic();
        assertEquals(queue.getStatus(), ServiceStatus.ALIVE);

        assertEquals(queue.process(messageSet).getResultCode(), ResultCode.OK);

        injector.getInstance(LifecycleManager.class).close();
    }
}
