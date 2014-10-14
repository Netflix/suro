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

package com.netflix.suro.sink;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.suro.SuroPlugin;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.MessageContainer;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class TestSinkManager {
    
    private Map<String, Sink> getSinkMap(ObjectMapper mapper, String desc) throws Exception {
        return mapper.<Map<String, Sink>>readValue(
                desc,
                new TypeReference<Map<String, Sink>>() {});
    }
    
    public static class TestSink implements Sink {
        public static final String TYPE = "TestSink";
        
        private final String message;
        private String status;
        private static AtomicInteger numOfSink = new AtomicInteger();

        public static int getNumOfSinks() {
            return numOfSink.get();
        }

        @JsonCreator
        public TestSink(@JsonProperty("message") String message) {
            this.message = message;
        }

        @Override
        public void writeTo(MessageContainer message) {}

        @Override
        public void open() {
            status = "open";
            numOfSink.incrementAndGet();
        }

        @Override
        public void close() {
            status = "closed";
            numOfSink.decrementAndGet();
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
    }

    @Test
    public void test() throws Exception {
        Injector injector = LifecycleInjector.builder().withModules(
                new SuroPlugin() {
                    @Override
                    protected void configure() {
                        this.addSinkType("TestSink", TestSink.class);
                    }
                },
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(ObjectMapper.class).to(DefaultObjectMapper.class);
                    }
                }
            ).build().createInjector();

        LifecycleManager lifecycleManager = injector.getInstance(LifecycleManager.class);
        lifecycleManager.start();

        String desc = "{\n" +
                "    \"default\": {\n" +
                "        \"type\": \"TestSink\",\n" +
                "        \"message\": \"defaultTestSink\"\n" +
                "    },\n" +
                "    \"topic1\": {\n" +
                "        \"type\": \"TestSink\",\n" +
                "        \"message\": \"topic1TestSink\"\n" +
                "    }\n" +
                "}";
        
        SinkManager sinkManager = injector.getInstance(SinkManager.class);
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        
        sinkManager.set(getSinkMap(mapper, desc));
        assertEquals(sinkManager.getSink("topic1").getStat(), "topic1TestSink open");
        assertEquals(sinkManager.getSink("default").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("topic7").getStat(), "defaultTestSink open");
        assertTrue(
                sinkManager.reportSinkStat().equals("default:defaultTestSink open\n\ntopic1:topic1TestSink open\n\n") ||
                sinkManager.reportSinkStat().equals("topic1:topic1TestSink open\n\ndefault:defaultTestSink open\n\n"));
        assertEquals(TestSink.getNumOfSinks(), 2);


        // change desc - test removal
        desc = "{\n" +
                "    \"default\": {\n" +
                "        \"type\": \"TestSink\",\n" +
                "        \"message\": \"defaultTestSink\"\n" +
                "    }\n" +
                "}";
        sinkManager.set(getSinkMap(mapper, desc));
        assertEquals(sinkManager.getSink("topic1").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("default").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.reportSinkStat(),
                String.format("default:defaultTestSink open\n\n"));
        assertEquals(TestSink.getNumOfSinks(), 1);

        // change desc - test addition
        desc = "{\n" +
                "    \"default\": {\n" +
                "        \"type\": \"TestSink\",\n" +
                "        \"message\": \"defaultTestSink\"\n" +
                "    },\n" +
                "    \"topic2\": {\n" +
                "        \"type\": \"TestSink\",\n" +
                "        \"message\": \"topic2TestSink\"\n" +
                "    }\n" +
                "}";
        sinkManager.set(getSinkMap(mapper, desc));
        assertEquals(sinkManager.getSink("topic1").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("default").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("topic2").getStat(), "topic2TestSink open");
        assertTrue(
                sinkManager.reportSinkStat().equals("default:defaultTestSink open\n\ntopic2:topic2TestSink open\n\n") ||
                        sinkManager.reportSinkStat().equals("topic2:topic2TestSink open\n\ndefault:defaultTestSink open\n\n")
        );
        assertEquals(TestSink.getNumOfSinks(), 2);

        // test exception - nothing changed
        desc = "{\n" +
                "    \"default\": {\n" +
                "        \"type\": \"TestSink\",\n" +
                "        \"message\": \"defaultTestSink\"\n" +
                "    },\n" +
                "    \"topic2\": {\n" +
                "        \"type\": \"TestSink\",\n" +
                "        \"message\": \"topic2TestSink\"\n" +
                "    }\n" +
                "},";
        sinkManager.set(getSinkMap(mapper, desc));
        assertEquals(sinkManager.getSink("topic1").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("default").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("topic2").getStat(), "topic2TestSink open");
        assertTrue(
                sinkManager.reportSinkStat().equals("default:defaultTestSink open\n\ntopic2:topic2TestSink open\n\n") ||
                        sinkManager.reportSinkStat().equals("topic2:topic2TestSink open\n\ndefault:defaultTestSink open\n\n")
        );
        assertEquals(TestSink.getNumOfSinks(), 2);

        // test destroy
        lifecycleManager.close();
        assertEquals(TestSink.getNumOfSinks(), 0);
        assertNull(sinkManager.getSink("any"));
    }
}
