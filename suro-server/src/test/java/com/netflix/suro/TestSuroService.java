package com.netflix.suro;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.inject.Injector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.suro.input.DynamicPropertyInputConfigurator;
import com.netflix.suro.input.SuroInput;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.routing.DynamicPropertyRoutingMapConfigurator;
import com.netflix.suro.sink.DynamicPropertySinkConfigurator;
import com.netflix.suro.sink.Sink;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;

public class TestSuroService {
    private static final AtomicBoolean sinkOpened = new AtomicBoolean(false);
    private static final AtomicBoolean inputOpened = new AtomicBoolean(false);
    private static final CountDownLatch latch = new CountDownLatch(2);

    public static class TestSuroServiceSink implements Sink {
        @JsonCreator
        public TestSuroServiceSink() {}

        @Override
        public void writeTo(MessageContainer message) {
        }

        @Override
        public void open() {
            latch.countDown();
            sinkOpened.set(true);
        }

        @Override
        public void close() {
        }

        @Override
        public String recvNotice() {
            return null;
        }

        @Override
        public String getStat() {
            return null;
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

    public static class TestSuroServiceInput implements SuroInput {
        @JsonCreator
        public TestSuroServiceInput() {}

        @Override
        public String getId() {
            return "testsuroservice";
        }

        @Override
        public void start() throws Exception {
            latch.countDown();
            assertTrue(sinkOpened.get());
            inputOpened.set(true);
        }

        @Override
        public void shutdown() {
        }

        @Override
        public void setPause(long ms) {

        }
    }

    @Test
    public void test() throws Exception {
        AtomicReference<Injector> injector = new AtomicReference<Injector>();
        Properties properties = new Properties();
        properties.setProperty(DynamicPropertyRoutingMapConfigurator.ROUTING_MAP_PROPERTY, "{}");
        properties.setProperty(DynamicPropertySinkConfigurator.SINK_PROPERTY,
                "{\n" +
                        "    \"default\": {\n" +
                        "        \"type\": \"testsuroservice\"\n" +
                        "        }\n" +
                        "    }\n" +
                        "}");
        properties.setProperty(DynamicPropertyInputConfigurator.INPUT_CONFIG_PROPERTY,
                "[\n" +
                        "    {\n" +
                        "        \"type\": \"testsuroservice\"\n" +
                        "    }\n" +
                        "]");
        SuroServer.create(injector, properties, new SuroPlugin() {
            @Override
            protected void configure() {
                addInputType("testsuroservice", TestSuroServiceInput.class);
                addSinkType("testsuroservice", TestSuroServiceSink.class);
            }
        });

        injector.get().getInstance(LifecycleManager.class).start();
        latch.await(5000, TimeUnit.MILLISECONDS);
        assertTrue(inputOpened.get());
        assertTrue(sinkOpened.get());
    }
}
