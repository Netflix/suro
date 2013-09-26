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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.Message;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestSinkManager {
    
    private static ObjectMapper jsonMapper = new DefaultObjectMapper();
    
    static {
        jsonMapper.registerSubtypes(new NamedType(TestSink.class, "TestSink"));
    }
    
    private static int numOfSinks = 0;

    public static class TestSink implements Sink {
        private final String message;
        private String status;

        @JsonCreator
        public TestSink(@JsonProperty("message") String message) {
            this.message = message;
        }

        @Override
        public void writeTo(Message message) {}

        @Override
        public void open() {
            status = "open";
            ++numOfSinks;
        }

        @Override
        public void close() {
            status = "closed";
            --numOfSinks;
        }

        @Override
        public String recvNotify() {
            return null;
        }

        @Override
        public String getStat() {
            return message + " " + status;
        }
    }

    private Map<String, Sink> getSinkMap(String desc) throws Exception {
        return jsonMapper.<Map<String, Sink>>readValue(
                desc,
                new TypeReference<Map<String, Sink>>() {});
    }
    
    @Test
    public void test() throws Exception {
        ObjectMapper jsonMapper = new DefaultObjectMapper();
        jsonMapper.registerSubtypes(new NamedType(TestSink.class, "TestSink"));

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
        
        SinkManager sinkManager = new SinkManager();
        
        sinkManager.set(getSinkMap(desc));
        assertEquals(sinkManager.getSink("topic1").getStat(), "topic1TestSink open");
        assertEquals(sinkManager.getSink("default").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("topic7").getStat(), "defaultTestSink open");
        assertTrue(
                sinkManager.reportSinkStat().equals("default:defaultTestSink open\n\ntopic1:topic1TestSink open\n\n") ||
                sinkManager.reportSinkStat().equals("topic1:topic1TestSink open\n\ndefault:defaultTestSink open\n\n"));
        assertEquals(numOfSinks, 2);


        // change desc - test removal
        desc = "{\n" +
                "    \"default\": {\n" +
                "        \"type\": \"TestSink\",\n" +
                "        \"message\": \"defaultTestSink\"\n" +
                "    }\n" +
                "}";
        sinkManager.set(getSinkMap(desc));
        assertEquals(sinkManager.getSink("topic1").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("default").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.reportSinkStat(),
                String.format("default:defaultTestSink open\n\n"));
        assertEquals(numOfSinks, 1);

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
        sinkManager.set(getSinkMap(desc));
        assertEquals(sinkManager.getSink("topic1").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("default").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("topic2").getStat(), "topic2TestSink open");
        assertTrue(
                sinkManager.reportSinkStat().equals("default:defaultTestSink open\n\ntopic2:topic2TestSink open\n\n") ||
                        sinkManager.reportSinkStat().equals("topic2:topic2TestSink open\n\ndefault:defaultTestSink open\n\n"));
        assertEquals(numOfSinks, 2);

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
        sinkManager.set(getSinkMap(desc));
        assertEquals(sinkManager.getSink("topic1").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("default").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("topic2").getStat(), "topic2TestSink open");
        assertTrue(
                sinkManager.reportSinkStat().equals("default:defaultTestSink open\n\ntopic2:topic2TestSink open\n\n") ||
                        sinkManager.reportSinkStat().equals("topic2:topic2TestSink open\n\ndefault:defaultTestSink open\n\n"));
        assertEquals(numOfSinks, 2);

        // test destroy
        sinkManager.shutdown();
        assertEquals(numOfSinks, 0);
        assertNull(sinkManager.getSink("any"));
    }
}
