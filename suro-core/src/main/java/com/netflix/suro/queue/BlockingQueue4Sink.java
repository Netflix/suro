package com.netflix.suro.queue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.suro.message.Message;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@NotThreadSafe
public class BlockingQueue4Sink implements MessageQueue4Sink {
    public static final String TYPE = "blocking";

    private final BlockingQueue<Message> queue;

    @JsonCreator
    public BlockingQueue4Sink(@JsonProperty("capacity") int capacity) {
        this.queue = new ArrayBlockingQueue(capacity);
    }

    @Override
    public boolean offer(Message msg) {
        try {
            queue.put(msg);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public Message poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return queue.poll(timeout, timeUnit);
    }

    @Override
    public int drain(int batchSize, List<Message> msgList) {
        return queue.drainTo(msgList, batchSize);
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public long size() {
        return queue.size();
    }
}
