package com.netflix.suro.sink;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.serde.SerDe;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestSinkManager {
    private static int numOfSinks = 0;

    public static class TestSink implements Sink {
        private final String message;
        private String status;

        @JsonCreator
        public TestSink(@JsonProperty("message") String message) {
            this.message = message;
        }

        @Override
        public void writeTo(Message message, SerDe serde) {
            return;
        }

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

    @Test
    public void test() throws Exception {
        Injector injector = LifecycleInjector.builder().withModules(new AbstractModule() {
            @Override
            protected void configure() {
                ObjectMapper jsonMapper = new ObjectMapper();
                jsonMapper.registerSubtypes(new NamedType(TestSink.class, "TestSink"));
                bind(ObjectMapper.class).toInstance(jsonMapper);
            }
        }).createInjector();
        LifecycleManager manager = injector.getInstance(LifecycleManager.class);
        manager.start();

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
        sinkManager.build(desc);
        assertEquals(sinkManager.getSink("topic1").getStat(), "topic1TestSink open");
        assertEquals(sinkManager.getSink("default").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("topic7").getStat(), "defaultTestSink open");
        assertTrue(
                sinkManager.reportSinkStat().equals("default:defaultTestSink open\ntopic1:topic1TestSink open\n") ||
                sinkManager.reportSinkStat().equals("topic1:topic1TestSink open\ndefault:defaultTestSink open\n"));
        assertEquals(numOfSinks, 2);


        // change desc - test removal
        desc = "{\n" +
                "    \"default\": {\n" +
                "        \"type\": \"TestSink\",\n" +
                "        \"message\": \"defaultTestSink\"\n" +
                "    }\n" +
                "}";
        sinkManager.build(desc);
        assertEquals(sinkManager.getSink("topic1").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("default").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.reportSinkStat(),
                String.format("default:defaultTestSink open\n"));
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
        sinkManager.build(desc);
        assertEquals(sinkManager.getSink("topic1").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("default").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("topic2").getStat(), "topic2TestSink open");
        assertTrue(
                sinkManager.reportSinkStat().equals("default:defaultTestSink open\ntopic2:topic2TestSink open\n") ||
                        sinkManager.reportSinkStat().equals("topic2:topic2TestSink open\ndefault:defaultTestSink open\n"));
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
        sinkManager.build(desc);
        assertEquals(sinkManager.getSink("topic1").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("default").getStat(), "defaultTestSink open");
        assertEquals(sinkManager.getSink("topic2").getStat(), "topic2TestSink open");
        assertTrue(
                sinkManager.reportSinkStat().equals("default:defaultTestSink open\ntopic2:topic2TestSink open\n") ||
                        sinkManager.reportSinkStat().equals("topic2:topic2TestSink open\ndefault:defaultTestSink open\n"));
        assertEquals(numOfSinks, 2);

        // test destroy
        sinkManager.shutdown();
        assertEquals(numOfSinks, 0);
        assertNull(sinkManager.getSink("any"));
    }
}
