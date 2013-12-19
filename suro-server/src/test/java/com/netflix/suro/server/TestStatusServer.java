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

package com.netflix.suro.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.netflix.governator.configuration.PropertiesConfigurationProvider;
import com.netflix.governator.guice.BootstrapBinder;
import com.netflix.governator.guice.BootstrapModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.suro.SuroDynamicPropertyModule;
import com.netflix.suro.SuroModule;
import com.netflix.suro.SuroPlugin;
import com.netflix.suro.routing.RoutingMap;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.SinkManager;
import com.netflix.suro.sink.TestSinkManager.TestSink;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TestStatusServer {
    private static LifecycleManager manager;
    private static Injector injector;
    private static final int statusPort = 7103;
    private static final int port = 7101;

    @BeforeClass
    public static void start() throws Exception {
        final Properties props = new Properties();
        props.put("SuroServer.statusServerPort", Integer.toString(statusPort));
        props.put("SuroServer.port", Integer.toString(port));

        // Create the injector
        injector = LifecycleInjector.builder()
                .withBootstrapModule(
                        new BootstrapModule() {
                            @Override
                            public void configure(BootstrapBinder binder) {
                                binder.bindConfigurationProvider().toInstance(
                                        new PropertiesConfigurationProvider(props));
                            }
                        }
                )
                .withModules(
                        new SuroModule(),
                        new SuroDynamicPropertyModule(),
                        new SuroPlugin() {
                            @Override
                            protected void configure() {
                                this.addSinkType("TestSink", TestSink.class);
                            }
                        },
                        StatusServer.createJerseyServletModule()
                )
                .createInjector();
        
        SinkManager  sinkManager = injector.getInstance(SinkManager.class);
        RoutingMap   routes      = injector.getInstance(RoutingMap.class);
        ObjectMapper mapper      = injector.getInstance(ObjectMapper.class);
        
        manager = injector.getInstance(LifecycleManager.class);
        manager.start(); // start status server with lifecycle manager

        String sinkDesc = "{\n" +
                "    \"default\": {\n" +
                "        \"type\": \"TestSink\",\n" +
                "        \"message\": \"default\"\n" +
                "    },\n" +
                "    \"sink1\": {\n" +
                "        \"type\": \"TestSink\",\n" +
                "        \"message\": \"sink1\"\n" +
                "    }\n" +
                "}";
        sinkManager.set((Map<String, Sink>)mapper.readValue(sinkDesc, new TypeReference<Map<String, Sink>>(){}));
        
        Thread.sleep(TimeUnit.SECONDS.toMillis(3));
    }

    @AfterClass
    public static void shutdown() {
        StatusServer server = injector.getInstance(StatusServer.class);
        server.shutdown();
        manager.close();
    }

    @Test
    public void connectionFailureShouldBeDetected() throws Exception {
        injector.getInstance(ThriftServer.class).shutdown();

        HttpResponse response = runQuery("healthcheck");

        assertEquals(500, response.getStatusLine().getStatusCode());

        injector.getInstance(ThriftServer.class).start();
    }

    private HttpResponse runQuery(String path) throws IOException {
        DefaultHttpClient client = new DefaultHttpClient();
        HttpGet httpget = new HttpGet(String.format("http://localhost:%d/%s", statusPort, path));

        try{
            return client.execute(httpget);
        } finally{
            client.getConnectionManager().shutdown();
        }
    }

    @Test
    public void healthcheckShouldPassForHealthyServer() throws Exception {
        HttpResponse response = runQuery("healthcheck");
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testSinkStat() throws IOException {
        HttpResponse response = runQuery("sinkstat");
        InputStream data = response.getEntity().getContent();
        BufferedReader br = new BufferedReader(new InputStreamReader(data));
        String line = null;
        StringBuilder sb = new StringBuilder();
        try {
            while ((line = br.readLine()) != null) {
                sb.append(line).append('\n');
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertEquals(sb.toString(), "sink1:sink1 open\n\ndefault:default open\n\n");
    }
}
