package com.netflix.suro.sink;

import com.google.common.collect.ImmutableMap;
import com.netflix.config.ConfigurationManager;
import com.netflix.suro.message.MessageContainer;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class TestSinkManager {

    private static class MockSink implements Sink {
        private final AtomicInteger openAttempts = new AtomicInteger(0);
        private volatile boolean isOpened = false;

        @Override
        public void writeTo(MessageContainer message) {

        }

        @Override
        public void open() {
            if(openAttempts.incrementAndGet() >= 3) {
                isOpened = true;
            } else {
                throw new RuntimeException("cannot open sink");
            }
        }

        @Override
        public void close() {
            isOpened = false;
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
        public boolean isOpened() {
            return isOpened;
        }
    }

    @Test
    public void testSinkOpenFailure() throws Exception {
        // override the interval from default 60s to 1s
        ConfigurationManager.getConfigInstance().setProperty("suro.SinkManager.sinkCheckInterval", "1");
        SinkManager sinkManager = new SinkManager();

        MockSink sink1 = new MockSink();
        sinkManager.initialSet(ImmutableMap.<String, Sink>of("sink1", sink1));
        sinkManager.initialStart();

        Assert.assertFalse(sink1.isOpened());
        Assert.assertEquals(1, sink1.openAttempts.get());

        Thread.sleep(1000 * 5);

        Assert.assertTrue(sink1.isOpened());
        Assert.assertEquals(3, sink1.openAttempts.get());

        MockSink sink2 = new MockSink();
        sinkManager.set(ImmutableMap.<String, Sink>of("sink2", sink2));

        Assert.assertFalse(sink1.isOpened());
        Assert.assertEquals(3, sink1.openAttempts.get());

        Assert.assertFalse(sink2.isOpened());
        Assert.assertEquals(1, sink2.openAttempts.get());

        Thread.sleep(1000 * 5);

        Assert.assertTrue(sink2.isOpened());
        Assert.assertEquals(3, sink2.openAttempts.get());
    }
}
