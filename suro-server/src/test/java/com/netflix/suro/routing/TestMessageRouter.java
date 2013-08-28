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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.netflix.governator.configuration.PropertiesConfigurationProvider;
import com.netflix.governator.guice.BootstrapBinder;
import com.netflix.governator.guice.BootstrapModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageSetBuilder;
import com.netflix.suro.message.serde.SerDe;
import com.netflix.suro.queue.MessageQueue;
import com.netflix.suro.server.ServerConfig;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.SinkManager;
import com.netflix.suro.thrift.TMessageSet;
import org.apache.thrift.TException;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertTrue;

public class TestMessageRouter {
    public static Map<String, Integer> messageCount = new HashMap<String, Integer>();

    public static class TestSink implements Sink {
        private final String message;
        private String status;
        private final List<String> messageList;

        @JsonCreator
        public TestSink(@JsonProperty("message") String message) {
            this.message = message;
            messageList = new LinkedList<String>();
        }

        @Override
        public void writeTo(Message message, SerDe serde) {
            Integer count = messageCount.get(this.message);
            if (count == null) {
                messageCount.put(this.message, 1);
            } else {
                messageCount.put(this.message, count + 1);
            }
            messageList.add(serde.toString(message.getPayload()));
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
        public String recvNotify() {
            return null;
        }

        @Override
        public String getStat() {
            return message + " " + status;
        }

        public List<String> getMessageList() {
            return messageList;
        }
    }

    @Test
    public void test() throws TException, InterruptedException {
        final Properties properties = new Properties();
        properties.setProperty(ServerConfig.MESSAGE_ROUTER_THREADS, "1");

        Injector injector = LifecycleInjector.builder().withBootstrapModule(new BootstrapModule() {
            @Override
            public void configure(BootstrapBinder binder) {
                binder.bindConfigurationProvider().toInstance(new PropertiesConfigurationProvider(properties));
                binder.bind(new TypeLiteral<BlockingQueue<TMessageSet>>() {})
                        .toProvider(new Provider<LinkedBlockingQueue<TMessageSet>>() {
                            @Override
                            public LinkedBlockingQueue<TMessageSet> get() {
                                return new LinkedBlockingQueue<TMessageSet>(
                                        Integer.parseInt(
                                                properties.getProperty(
                                                        ServerConfig.MEMORY_QUEUE_SIZE, "100"))
                                );
                            }
                        });
                ObjectMapper jsonMapper = new DefaultObjectMapper();
                jsonMapper.registerSubtypes(new NamedType(TestSink.class, "TestSink"));
                binder.bind(ObjectMapper.class).toInstance(jsonMapper);
            }
        }).createInjector();

        SinkManager sinkManager = startSinkMakager(injector);

        MessageRouter router = startMessageRouter(injector);

        MessageQueue queue = injector.getInstance(MessageQueue.class);

        MessageSetBuilder builder = new MessageSetBuilder();
        builder = builder.withApp("app").withHostname("testhost");

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

        for (int i = 0; i < 20; ++i) {
            builder.withMessage("topic3", Integer.toString(i).getBytes());
        }
        // default: 20
        queue.process(builder.build());

        // total sink1: 15, default: 30
        int count = 10;
        while (answer() == false && count > 0) {
            Thread.sleep(1000);
            --count;
        }
        assertTrue(count > 0);

        router.shutdown();
        sinkManager.shutdown();
    }

    public static MessageRouter startMessageRouter(Injector injector) {
        String mapDesc = "{\n" +
                "    \"topic1\": {\n" +
                "        \"where\": [\n" +
                "            \"sink1\",\n" +
                "            \"default\"\n" +
                "        ]\n" +
                "    },\n" +
                "    \"topic2\": {\n" +
                "        \"where\": [\n" +
                "            \"sink1\"\n" +
                "        ]\n" +
                "    }\n" +
                "}";
        RoutingMap routingMap = injector.getInstance(RoutingMap.class);
        routingMap.build(mapDesc);
        MessageRouter router = injector.getInstance(MessageRouter.class);
        router.start();
        return router;
    }

    public static SinkManager startSinkMakager(Injector injector) {
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
        sinkManager.build(sinkDesc);

        return sinkManager;
    }

    private boolean answer() {
        Integer sink1 = messageCount.get("sink1");
        Integer defaultV = messageCount.get("default");
        if (sink1 != null && sink1 == 15 && defaultV != null && defaultV == 30) {
            return true;
        } else {
            return false;
        }
    }
}
