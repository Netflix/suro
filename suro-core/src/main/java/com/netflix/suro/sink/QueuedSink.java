package com.netflix.suro.sink;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.suro.TagKey;
import com.netflix.suro.message.Message;
import com.netflix.suro.queue.MessageQueue4Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Asynchronous sink would have the internal buffer to return on
 * {@link Sink#writeTo(com.netflix.suro.message.MessageContainer)} method.
 * Implementing asynchronous sink can be done by simply extending this class
 * and initialize itself with {@link MessageQueue4Sink}. This is extending
 * {@link Thread} and its start() method should be called.
 *
 * @author jbae
 */
public abstract class QueuedSink extends Thread {
    protected static Logger log = LoggerFactory.getLogger(QueuedSink.class);

    private long lastBatch = System.currentTimeMillis();
    protected volatile boolean isRunning = false;
    private volatile boolean isStopped = false;

    @VisibleForTesting
    protected MessageQueue4Sink queue4Sink;
    private int batchSize;
    private int batchTimeout;
    private String sinkId;

    public String getSinkId() { return sinkId; }

    protected void initialize(String sinkId, MessageQueue4Sink queue4Sink, int batchSize, int batchTimeout) {
        this.sinkId = sinkId;
        this.queue4Sink = queue4Sink;
        this.batchSize = batchSize == 0 ? 1000 : batchSize;
        this.batchTimeout = batchTimeout == 0 ? 1000 : batchTimeout;
    }

    protected void initialize(MessageQueue4Sink queue4Sink, int batchSize, int batchTimeout) {
        this.sinkId = "empty_sink_id";
        this.queue4Sink = queue4Sink;
        this.batchSize = batchSize == 0 ? 1000 : batchSize;
        this.batchTimeout = batchTimeout == 0 ? 1000 : batchTimeout;
    }

    @VisibleForTesting
    protected AtomicLong droppedMessagesCount = new AtomicLong(0);

    protected void enqueue(Message message) {
        if (!queue4Sink.offer(message)) {
            droppedMessagesCount.incrementAndGet();
            DynamicCounter.increment(
                    MonitorConfig.builder(TagKey.DROPPED_COUNT)
                            .withTag("reason", "queueFull")
                            .withTag("sink", sinkId)
                            .build());
        }
    }

    @Override
    public void run() {
        isRunning = true;
        List<Message> msgList = new LinkedList<Message>();

        while (isRunning || !msgList.isEmpty()) {
            try {
                beforePolling();

                boolean full = (msgList.size() >= batchSize);
                boolean expired = false;

                if (!full) {
                    Message msg = queue4Sink.poll(
                            Math.max(0, lastBatch + batchTimeout - System.currentTimeMillis()),
                            TimeUnit.MILLISECONDS);
                    expired = (msg == null);
                    if (!expired) {
                        msgList.add(msg);
                        queue4Sink.drain(batchSize - msgList.size(), msgList);
                    }
                }
                full = (msgList.size() >= batchSize);
                if (expired || full) {
                    if (msgList.size() > 0) {
                        write(msgList);
                        msgList.clear();
                    }
                    lastBatch = System.currentTimeMillis();
                }
            } catch (Throwable e) {
                log.error("Thrown on running: " + e.getMessage(), e);
                droppedMessagesCount.addAndGet(msgList.size());
                DynamicCounter.increment(
                        MonitorConfig.builder(TagKey.DROPPED_COUNT)
                                .withTag("reason", "sinkException")
                                .withTag("sink", sinkId)
                                .build(),
                        msgList.size());
                msgList.clear(); // drop messages, otherwise, it will retry forever
            }
        }
        log.info("Shutdown request exit loop ..., queue.size at exit time: " + queue4Sink.size());
        try {
            while (!queue4Sink.isEmpty()) {
                if (queue4Sink.drain(batchSize, msgList) > 0) {
                    write(msgList);
                    msgList.clear();
                }
            }

            log.info("Shutdown request internalClose done ...");
        } catch (Exception e) {
            log.error("Exception on terminating: " + e.getMessage(), e);
        } finally {
            isStopped = true;
        }
    }

    public void close() {
        isRunning = false;
        log.info("Starting to close and set isRunning to false");
        do {
            try {
                Thread.sleep(500);
                isRunning = false;
            } catch (Exception ignored) {
                log.error("ignoring an exception on close");
            }
        } while (!isStopped);

        try {
            queue4Sink.close();
            innerClose();
        } catch (Exception e) {
            // ignore exceptions when closing
            log.error("Exception while closing: " + e.getMessage(), e);
        } finally {
            log.info("close finished");
        }
    }

    /**
     * Some preparation job before polling messages from the queue
     *
     * @throws IOException
     */
    abstract protected void beforePolling() throws IOException;

    /**
     * Actual writing to the sink
     *
     * @param msgList
     * @throws IOException
     */
    abstract protected void write(List<Message> msgList) throws IOException;

    /**
     * Actual close implementation
     *
     * @throws IOException
     */
    abstract protected void innerClose() throws IOException;

    @Monitor(name = "numOfPendingMessages", type = DataSourceType.GAUGE)
    public long getNumOfPendingMessages() {
        return queue4Sink.size();
    }
}
