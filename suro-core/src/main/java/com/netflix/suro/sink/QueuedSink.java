package com.netflix.suro.sink;

import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.suro.message.Message;
import com.netflix.suro.queue.MessageQueue4Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
    static Logger log = LoggerFactory.getLogger(QueuedSink.class);

    private long lastBatch = System.currentTimeMillis();
    protected volatile boolean isRunning = false;
    private volatile boolean isStopped = false;

    protected MessageQueue4Sink queue4Sink;
    private int batchSize;
    private int batchTimeout;

    protected void initialize(MessageQueue4Sink queue4Sink, int batchSize, int batchTimeout) {
        this.queue4Sink = queue4Sink;
        this.batchSize = batchSize == 0 ? 1000 : batchSize;
        this.batchTimeout = batchTimeout == 0 ? 1000 : batchTimeout;
    }

    @Monitor(name = "queueSize", type = DataSourceType.GAUGE)
    public long getQueueSize() {
        return queue4Sink.size();
    }

    @Override
    public void run() {
        isRunning = true;
        List<Message> msgList = new LinkedList<Message>();

        while (isRunning || !msgList.isEmpty()) {
            try {
                beforePolling();

                Message msg = queue4Sink.poll(
                        Math.max(0, lastBatch + batchTimeout - System.currentTimeMillis()),
                        TimeUnit.MILLISECONDS);
                boolean expired = (msg == null);
                if (!expired) {
                    msgList.add(msg);
                    queue4Sink.drain(batchSize - msgList.size(), msgList);
                }
                boolean full = (msgList.size() >= batchSize);
                if (expired || full) {
                    if (msgList.size() > 0) {
                        write(msgList);
                    }
                    lastBatch = System.currentTimeMillis();
                }
            } catch (Exception e) {
                log.error("Exception on running: " + e.getMessage(), e);
            }
        }
        log.info("Shutdown request exit loop ..., queue.size at exit time: " + queue4Sink.size());
        try {
            while (!queue4Sink.isEmpty()) {
                if (queue4Sink.drain(batchSize, msgList) > 0) {
                    write(msgList);
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
            innerClose();
        } catch (IOException e) {
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
}
