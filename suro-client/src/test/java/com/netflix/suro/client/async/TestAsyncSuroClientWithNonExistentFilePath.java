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
import com.google.inject.ProvisionException;
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
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestAsyncSuroClientWithNonExistentFilePath {
    private Injector injector;
    private List<SuroServer4Test> servers;
    private final static String NON_EXISTENT_PATH = "/tmp/should_not_existing_at_all_"+System.currentTimeMillis();
    private final static String TEMP_FILE = "/tmp/tempFile";

    private void setupFile(final Properties props, String filePath) throws Exception {
        servers = TestConnectionPool.startServers(3);

        props.put(ClientConfig.LB_SERVER, TestConnectionPool.createConnectionString(servers));

        props.put(ClientConfig.ASYNC_FILEQUEUE_PATH, filePath);
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
        FileUtils.deleteDirectory(new File(NON_EXISTENT_PATH));
        FileUtils.deleteQuietly(new File(TEMP_FILE));

        TestConnectionPool.shutdownServers(servers);
        injector.getInstance(LifecycleManager.class).close();

        assertFalse(String.format("The directory %s should be deleted", NON_EXISTENT_PATH), new File(NON_EXISTENT_PATH).exists());
    }

    @Test
    public void testFileBaseQueueShouldCreateNonExistentFile() throws Exception {
        assertFalse(String.format("The file %s shouldn't exist", NON_EXISTENT_PATH), new File(NON_EXISTENT_PATH).exists());

        setupFile(new Properties(), NON_EXISTENT_PATH);

        AsyncSuroClient client = injector.getInstance(AsyncSuroClient.class);

        assertTrue(String.format("The path %s should be created", NON_EXISTENT_PATH), new File(NON_EXISTENT_PATH).exists());
        assertTrue(String.format("The path %s should be a directory", NON_EXISTENT_PATH), new File(NON_EXISTENT_PATH).isDirectory());

        for (int i = 0; i < 3000; ++i) {
            client.send(new Message("routingKey", "testMessage".getBytes()));
        }

        client.shutdown();
        TestConnectionPool.checkMessageCount(servers, 3000);

        assertEquals(client.getSentMessageCount(), 3000);
    }

    @Test
    public void testExistingFileWillResultInFailure() throws Exception {
        File tempFile = new File(TEMP_FILE);
        tempFile.createNewFile();
        assertTrue(String.format("The temp file %s should be created", tempFile), tempFile.exists() && tempFile.isFile());

        setupFile(new Properties(), TEMP_FILE);

        try{
            injector.getInstance(AsyncSuroClient.class);
            fail("The creation of Suro client should fail due to invalid path");
        }catch(ProvisionException e) {
            assertTrue("The unexpected error message: "+e.getMessage(), e.getMessage().contains("IllegalStateException"));
        }
    }
}
