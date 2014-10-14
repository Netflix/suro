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

package com.netflix.suro.routing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.netflix.governator.configuration.PropertiesConfigurationProvider;
import com.netflix.governator.guice.BootstrapBinder;
import com.netflix.governator.guice.BootstrapModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.SuroPlugin;
import com.netflix.suro.input.SuroInput;
import com.netflix.suro.input.thrift.MessageSetProcessor;
import com.netflix.suro.input.thrift.ServerConfig;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.message.MessageSetBuilder;
import com.netflix.suro.message.SerDe;
import com.netflix.suro.message.StringSerDe;
import com.netflix.suro.routing.RoutingMap.RoutingInfo;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.SinkManager;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestMessageRouter {
    public static Map<String, Integer> messageCount = new HashMap<String, Integer>();

    public static class TestMessageRouterSink implements Sink {
        private final String message;
        private String status;
        private final List<String> messageList;
        private SerDe<String> serde = new StringSerDe();

        @JsonCreator
        public TestMessageRouterSink(@JsonProperty("message") String message) {
            this.message = message;
            this.messageList = new LinkedList<String>();
        }

        @Override
        public synchronized void writeTo(MessageContainer message)  {
            try {
                System.out.println("message: " + this.message + " msg: " + message.getEntity(String.class));
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            Integer count = messageCount.get(this.message);
            if (count == null) {
                messageCount.put(this.message, 1);
            } else {
                messageCount.put(this.message, count + 1);
            }
            try {
                messageList.add(serde.deserialize(message.getEntity(byte[].class)));
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        }

        @Override
        public void open() {
            status = "open";
        }

        @Override
        public void close() {
            status = "closed";
        }

        @Override
        public String recvNotice() {
            return null;
        }

        @Override
        public String getStat() {
            return message + " " + status;
        }

        @Override
        public long getNumOfPendingMessages() {
            return 0;
        }

        @Override
        public long checkPause() {
            return 0;
        }

        public List<String> getMessageList() {
            return messageList;
        }
    }

    private static Map<String, Sink> getSinkMap(ObjectMapper jsonMapper, String desc) throws Exception {
        return jsonMapper.readValue(
            desc,
            new TypeReference<Map<String, Sink>>() {
            });
    }
    
    private static Map<String, RoutingInfo> getRoutingMap(ObjectMapper jsonMapper, String desc) throws Exception {
        return jsonMapper.readValue(
            desc,
            new TypeReference<Map<String, RoutingInfo>>() {
            });
    }

    @Test
    public void test() throws Exception {
        final Properties properties = new Properties();
        properties.setProperty(ServerConfig.MESSAGE_ROUTER_THREADS, "1");

        Injector injector = LifecycleInjector.builder()
            .withModules(
                new SuroPlugin() {
                    @Override
                    protected void configure() {
                        bind(ObjectMapper.class).to(DefaultObjectMapper.class);
                        this.addSinkType("TestSink", TestMessageRouterSink.class);
                    }
                },
                new RoutingPlugin()
            )
            .withBootstrapModule(new BootstrapModule() {
                @Override
                public void configure(BootstrapBinder binder) {
                    binder.bindConfigurationProvider().toInstance(new PropertiesConfigurationProvider(properties));
                }
        }).build().createInjector();
        
        
        SinkManager sinkManager = startSinkMakager(injector);

        startMessageRouter(injector);

        MessageSetProcessor queue = injector.getInstance(MessageSetProcessor.class);
        queue.setInput(mock(SuroInput.class));
        queue.start();

        MessageSetBuilder builder = new MessageSetBuilder(new ClientConfig());

        for (int i = 0; i < 10; ++i) {
            builder.withMessage("topic1", Integer.toString(i).getBytes());
        }
        // sink1: 10
        // default: 10
        queue.process(builder.build());

        for (int i = 0; i < 5; ++i) {
            builder.withMessage("topic2", Integer.toString(i).getBytes());
        }
        // sink1: 5
        queue.process(builder.build());

        for (int i = 0; i < 15; ++i) {
            builder.withMessage("topic3", Integer.toString(i).getBytes());
        }
        queue.process(builder.build());
        // sink3: 15 with topic3_alias

        for (int i = 0; i < 20; ++i) {
            builder.withMessage("topic4", Integer.toString(i).getBytes());
        }
        // default: 20
        queue.process(builder.build());

        // total sink1: 15, default: 30
        int count = 10;
        while (!answer() && count > 0) {
            Thread.sleep(1000);
            --count;
        }
        assertTrue(count > 0);

        queue.shutdown();
        sinkManager.shutdown();
    }

    public static MessageRouter startMessageRouter(Injector injector) throws Exception {
        String mapDesc = "{\n" +
                "    \"topic1\": {\n" +
                "        \"where\": [\n" +
                "            {\n" +
                "                \"sink\": \"sink1\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"sink\": \"default\"\n" +
                "            }\n" +
                "        ]\n" +
                "    },\n" +
                "    \"topic2\": {\n" +
                "        \"where\": [\n" +
                "            {\n" +
                "                \"sink\": \"sink1\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"sink\": \"sink2\",\n" +
                "                \"filter\": {\n" +
                "                    \"type\": \"regex\",\n" +
                "                    \"regex\": \"1\"\n" +
                "                }\n" +
                "            }\n" +
                "        ]\n" +
                "    },\n" +
                "    \"topic3\": {\n" +
                "        \"where\": [\n" +
                "            {\n" +
                "                \"sink\": \"sink3\",\n" +
                "                \"alias\": \"topic3_alias\"\n" +
                "            }\n" +
                "        ]\n" +
                "    }\n" +
                "}";
        
        RoutingMap routingMap = injector.getInstance(RoutingMap.class);
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        routingMap.set(getRoutingMap(mapper, mapDesc));
        return injector.getInstance(MessageRouter.class);
    }

    public static SinkManager startSinkMakager(Injector injector) throws Exception {
        String sinkDesc = "{\n" +
                "    \"default\": {\n" +
                "        \"type\": \"TestSink\",\n" +
                "        \"message\": \"default\"\n" +
                "    },\n" +
                "    \"sink1\": {\n" +
                "        \"type\": \"TestSink\",\n" +
                "        \"message\": \"sink1\"\n" +
                "    },\n" +
                "    \"sink2\": {\n" +
                "        \"type\": \"TestSink\",\n" +
                "        \"message\": \"sink2\"\n" +
                "    },\n" +
                "    \"sink3\": {\n" +
                "        \"type\": \"TestSink\",\n" +
                "        \"message\": \"sink3\"\n" +
                "    }\n" +
                "}";
        SinkManager sinkManager = injector.getInstance(SinkManager.class);
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        sinkManager.set(getSinkMap(mapper, sinkDesc));

        return sinkManager;
    }

    private boolean answer() {
        Integer sink1 = messageCount.get("sink1");
        Integer sink2 = messageCount.get("sink2");
        Integer sink3 = messageCount.get("sink3");
        Integer defaultV = messageCount.get("default");
        return sink1 != null && sink1 == 15 && defaultV != null && defaultV == 30 && sink2 == 1 && sink3 != null && sink3 == 15;
    }
}
