package com.netflix.suro.sink;

import com.netflix.suro.message.Message;
import com.netflix.suro.queue.FileQueue4Sink;
import com.netflix.suro.queue.MemoryQueue4Sink;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestQueuedSink {
    @Rule
    public TemporaryFolder folder= new TemporaryFolder();

    @Test
    public void testDrainOnce() throws IOException {
        FileQueue4Sink queue = new FileQueue4Sink(folder.newFolder().getAbsolutePath(), "testDrainOnce", "PT1m", 1024 * 1024 * 1024);
        final List<Message> sentMessageList = new LinkedList<Message>();

        QueuedSink sink = new QueuedSink() {
            @Override
            protected void beforePolling() throws IOException {

            }

            @Override
            protected void write(List<Message> msgList) throws IOException {
                sentMessageList.addAll(msgList);
                msgList.clear();

            }

            @Override
            protected void innerClose() throws IOException {

            }
        };
        sink.initialize(queue, 100, 1000);
        sink.start();

        int msgCount = 1000;
        for (int i = 0; i < msgCount; ++i) {
            queue.offer(new Message("routingKey", ("message" + i).getBytes()));
        }

        sink.close();

        assertEquals(sentMessageList.size(), msgCount);
    }

    @Test
    public void shouldIncrementDroppedCounter() throws InterruptedException {
        final int queueCapacity = 200;
        final MemoryQueue4Sink queue = new MemoryQueue4Sink(queueCapacity);

        QueuedSink sink = new QueuedSink() {
            @Override
            protected void beforePolling() throws IOException {
            }

            @Override
            protected void write(List<Message> msgList) throws IOException {
                throw new RuntimeException("prevent to drain the queue");
            }

            @Override
            protected void innerClose() throws IOException {
            }
        };
        sink.initialize(queue, 100, 1000);
        sink.start();

        int msgCount = 1000;
        for (int i = 0; i < msgCount; ++i) {
            sink.enqueue(new Message("routingKey", ("message" + i).getBytes()));
        }
        for (int i = 0; i < 10; ++i) {
            if (sink.droppedMessagesCount.get() < msgCount) {
                Thread.sleep(1000);
            }
        }
        sink.close();

        assertEquals(sink.droppedMessagesCount.get(), msgCount);
    }

    @Test
    public void synchronizedQueue() throws InterruptedException {
        final int queueCapacity = 0;
        final MemoryQueue4Sink queue = new MemoryQueue4Sink(queueCapacity);
        final AtomicInteger sentCount = new AtomicInteger();

        QueuedSink sink = new QueuedSink() {
            @Override
            protected void beforePolling() throws IOException {
            }

            @Override
            protected void write(List<Message> msgList) throws IOException {
                sentCount.addAndGet(msgList.size());
            }

            @Override
            protected void innerClose() throws IOException {
            }
        };
        sink.initialize(queue, 100, 1000);
        sink.start();

        int msgCount = 1000;
        int offered = 0;
        for (int i = 0; i < msgCount; ++i) {
            if (queue.offer(new Message("routingKey", ("message" + i).getBytes()))) {
                offered++;
            }
        }
        assertEquals(msgCount, offered);
        for (int i = 0; i < 20; ++i) {
            if (sentCount.get() < offered) {
                Thread.sleep(500);
            }
        }
        assertEquals(sentCount.get(), offered);
    }


    @Test
    public void shouldNotPauseOnShortQueue() {
        QueuedSink sink = new QueuedSink() {
            @Override
            protected void beforePolling() throws IOException {

            }

            @Override
            protected void write(List<Message> msgList) throws IOException {
            }

            @Override
            protected void innerClose() throws IOException {

            }
        };

        int queueCapacity = 10000;
        final MemoryQueue4Sink queue = new MemoryQueue4Sink(queueCapacity);
        final int initialCount = queueCapacity / 2 - 10;
        for (int i = 0; i < initialCount; ++i) {
            queue.offer(new Message("routingKey", ("testMessage" + i).getBytes()));
        }

        sink.initialize(queue, 100, 1000);

        assertEquals(sink.checkPause(), 0);
    }

    @Test
    public void shouldPauseOnLongQueue() throws InterruptedException {
        QueuedSink sink = new QueuedSink() {
            @Override
            protected void beforePolling() throws IOException {

            }

            @Override
            protected void write(List<Message> msgList) throws IOException {
            }

            @Override
            protected void innerClose() throws IOException {

            }
        };

        int queueCapacity = 10000;
        final MemoryQueue4Sink queue = new MemoryQueue4Sink(queueCapacity);
        final int initialCount = queueCapacity / 2 + 10;
        for (int i = 0; i < initialCount; ++i) {
            queue.offer(new Message("routingKey", ("testMessage" + i).getBytes()));
        }

        sink.initialize(null, queue, 100, 1000, true);

        assertEquals(sink.checkPause(), queue.size());

        queue.drain(Integer.MAX_VALUE, new LinkedList<Message>());

        ///////////////////////////
        sink = new QueuedSink() {
            @Override
            protected void beforePolling() throws IOException {

            }

            @Override
            protected void write(List<Message> msgList) throws IOException {
            }

            @Override
            protected void innerClose() throws IOException {

            }
        };

        QueuedSink.MAX_PENDING_MESSAGES_TO_PAUSE = 100;
        for (int i = 0; i < QueuedSink.MAX_PENDING_MESSAGES_TO_PAUSE + 1; ++i) {
            queue.offer(new Message("routingKey", ("testMessage" + i).getBytes()));
        }

        sink.initialize(null, queue, 100, 1000, true);

        assertEquals(sink.checkPause(), queue.size());

        QueuedSink.MAX_PENDING_MESSAGES_TO_PAUSE = 1000000;

        queue.drain(Integer.MAX_VALUE, new LinkedList<Message>());

        ////////////////////////////
        sink = new QueuedSink() {
            @Override
            protected void beforePolling() throws IOException {
            }

            @Override
            protected void write(List<Message> msgList) throws IOException {
            }

            @Override
            protected void innerClose() throws IOException {

            }
        };
        sink.initialize(null, queue, 100, 1000, true);
        sink.throughput.increment(initialCount);

        for (int i = 0; i < initialCount; ++i) {
            queue.offer(new Message("routingKey", ("testMessage" + i).getBytes()));
        }

        assertTrue(sink.checkPause() < queue.size() && sink.checkPause() > 0);
    }
}
