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
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.netflix.governator.guice.BootstrapBinder;
import com.netflix.governator.guice.BootstrapModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.routing.TestMessageRouter;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.SinkManager;
import com.netflix.suro.thrift.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;

public class TestStatusServer {
    private static class TestServer implements SuroServer.Iface {
        private Result result;
        public TestServer(Result result) {
            this.result = result;
        }

        @Override
        public Result process(TMessageSet messageSet) throws TException {
            result.setMessage(messageSet.getApp());
            result.setResultCode(ResultCode.OK);

            return result;
        }

        @Override
        public long shutdown() throws TException {
            return 0;
        }

        @Override
        public String getName() throws TException {
            return null;
        }

        @Override
        public ServiceStatus getStatus() throws TException {
            return ServiceStatus.ALIVE;
        }

        @Override
        public String getVersion() throws TException {
            return null;
        }
    }

    private static LifecycleManager manager;
    private static Injector injector;

    private static Map<String, Sink> getSinkMap(ObjectMapper jsonMapper, String desc) throws Exception {
        return jsonMapper.<Map<String, Sink>>readValue(
                desc,
                new TypeReference<Map<String, Sink>>() {});
    }
    

    @BeforeClass
    public static void start() throws Exception {
        injector = LifecycleInjector.builder().withBootstrapModule(new BootstrapModule() {
            @Override
            public void configure(BootstrapBinder binder) {
                binder.bind(new TypeLiteral<BlockingQueue<TMessageSet>>() {})
                        .toProvider(new Provider<LinkedBlockingQueue<TMessageSet>>() {
                            @Override
                            public LinkedBlockingQueue<TMessageSet> get() {
                                return new LinkedBlockingQueue<TMessageSet>(1);
                            }
                        });
                ObjectMapper jsonMapper = new DefaultObjectMapper();
                jsonMapper.registerSubtypes(new NamedType(TestMessageRouter.TestSink.class, "TestSink"));
                binder.bind(ObjectMapper.class).toInstance(jsonMapper);
            }
        }).withModules(StatusServer.createJerseyServletModule()).createInjector();

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
        SinkManager sinkManager = injector.getInstance(SinkManager.class);
        
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        sinkManager.set(getSinkMap(mapper, sinkDesc));

        StatusServer statusServer = injector.getInstance(StatusServer.class);
    }

    @AfterClass
    public static void shutdown() {
        StatusServer server = injector.getInstance(StatusServer.class);
        server.shutdown();
        manager.close();
    }

    @Test
    public void connectionFailureShouldBeDetected() throws Exception {
        HttpResponse response = runQuery("healthcheck");

        assertEquals(500, response.getStatusLine().getStatusCode());
    }

    private HttpResponse runQuery(String path) throws IOException {
        DefaultHttpClient client = new DefaultHttpClient();
        HttpGet httpget = new HttpGet(String.format("http://localhost:%d/%s", 7103, path));

        try{
            return client.execute(httpget);
        } finally{
            client.getConnectionManager().shutdown();
        }
    }

    @Test
    public void healthcheckShouldPassForHealthyServer() throws Exception {
        TNonblockingServerSocket transport = new TNonblockingServerSocket(7101);

        Result r = new Result();
        TestServer server = new TestServer(r);

        @SuppressWarnings({ "rawtypes", "unchecked" })
        TProcessor processor =  new SuroServer.Processor(server);

        final THsHaServer main = new THsHaServer(new THsHaServer.Args(transport).processor(processor));
        new Thread(new Runnable(){

            @Override
            public void run() {
                main.serve();
            }
        }).start();

        try{
            // 2 seconds should be enough for a simple server to start up
            Thread.sleep(2000);

            HttpResponse response = runQuery("healthcheck");
            assertEquals(200, response.getStatusLine().getStatusCode());
        } finally{
            main.stop();
        }
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
