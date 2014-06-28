package com.netflix.suro.sink;

import com.netflix.suro.message.Message;
import com.netflix.suro.queue.BlockingQueue4Sink;
import com.netflix.suro.queue.FileQueue4Sink;
import com.netflix.suro.queue.MemoryQueue4Sink;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestQueuedSink {
    @Rule
    public TemporaryFolder folder= new TemporaryFolder();

    @Test
    public void shouldStopDrainingOnWriteException() throws InterruptedException {
        final int msgCount = 1000;
        final CountDownLatch latch = new CountDownLatch(2);
        MemoryQueue4Sink queue = new MemoryQueue4Sink(10000);
        final List<Message> sentMessageList = new LinkedList<Message>();
        final List<Message> unsentMessageList = new LinkedList<Message>();
        final AtomicBoolean success = new AtomicBoolean(false);

        QueuedSink sink = new QueuedSink() {
            int exceptionCount = 10;

            @Override
            protected void beforePolling() throws IOException {

            }

            @Override
            protected void write(List<Message> msgList) throws IOException {
                if (sentMessageList.size() <= msgCount / 2 || success.get()) {
                    sentMessageList.addAll(msgList);
                    msgList.clear();
                } else {
                    if (latch.getCount() == 2) {
                        latch.countDown();
                        unsentMessageList.addAll(msgList);
                    }
                    --exceptionCount;
                    if (exceptionCount == 0) {
                        latch.countDown();
                    }
                    throw new RuntimeException("simulated");
                }
            }

            @Override
            protected void innerClose() throws IOException {

            }
        };

        sink.initialize(queue, 100, 1000);
        sink.start();

        for (int i = 0; i < msgCount; ++i) {
            queue.offer(new Message("routingKey", ("message" + i).getBytes()));
        }
        latch.await();

        assertEquals(queue.size(), msgCount - sentMessageList.size() - unsentMessageList.size());

        success.set(true);
        sink.close();

        assertEquals(queue.size(), 0);
        assertEquals(sentMessageList.size(), msgCount);
        // check no duplicate messages
        Set<String> msgSet = new HashSet<String>();
        for (Message m : sentMessageList) {
            msgSet.add(new String(m.getPayload()));
        }
        assertEquals(msgSet.size(), msgCount);
    }

    @Test
    public void testDrainOnce() throws IOException {
        FileQueue4Sink queue = new FileQueue4Sink(folder.newFolder().getAbsolutePath(), "testDrainOnce", "PT1h");
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
    public void shouldIncrementDroppedCounter() {
        int queueCapacity = 200;
        MemoryQueue4Sink queue = new MemoryQueue4Sink(queueCapacity);
        final List<Message> unsentMessageList = new LinkedList<Message>();

        QueuedSink sink = new QueuedSink() {
            @Override
            protected void beforePolling() throws IOException {
            }

            @Override
            protected void write(List<Message> msgList) throws IOException {
                if (unsentMessageList.isEmpty()) {
                    unsentMessageList.addAll(msgList);
                }
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

        assertEquals(sink.droppedMessagesCount.get(), msgCount - queueCapacity - unsentMessageList.size());
    }

    @Test
    public void shouldReturnPendingTasks() throws InterruptedException {
        int jobPoolSize = 100;
        int queueSize = 1;
        final CountDownLatch waitingLatch = new CountDownLatch(1);
        final CountDownLatch goLatch = new CountDownLatch(jobPoolSize);

        ThreadPoolQueuedSink sink = new ThreadPoolQueuedSink(jobPoolSize, 1, 1, Long.MAX_VALUE, "testqueuedsink") {
            @Override
            protected void beforePolling() throws IOException {

            }

            @Override
            protected void write(List<Message> msgList) throws IOException {
                senders.execute(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            waitingLatch.await(10, TimeUnit.SECONDS);
                            goLatch.countDown();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        };

        sink.initialize(new BlockingQueue4Sink(queueSize), 1, Integer.MAX_VALUE);
        sink.start();
        for (int i = 0; i < jobPoolSize; ++i) {
            sink.enqueue(new Message("routingKey", ("message" + i).getBytes()));
        }

        for (int i = 0; i < 10 && !sink.queue4Sink.isEmpty(); ++i) {
            Thread.sleep(1000);
        }
        assertEquals(sink.getJobQueueSize(), jobPoolSize - 1);
        assertEquals(sink.getNumOfPendingMessages(), jobPoolSize);

        waitingLatch.countDown();
        goLatch.await(10, TimeUnit.SECONDS);

        assertEquals(sink.getJobQueueSize(), 0);
        assertEquals(sink.getNumOfPendingMessages(), 0);
        assertTrue(sink.queue4Sink.isEmpty());
    }
}
