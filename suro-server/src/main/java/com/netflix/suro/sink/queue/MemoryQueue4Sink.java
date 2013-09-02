package com.netflix.suro.sink.queue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.suro.message.Message;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@NotThreadSafe
public class MemoryQueue4Sink implements MessageQueue4Sink {
    public static final String TYPE = "memory";

    private final BlockingQueue<Message> queue;

    @JsonCreator
    public MemoryQueue4Sink(@JsonProperty("capacity") int capacity) {
        this.queue = new LinkedBlockingQueue(capacity);
    }

    @Override
    public void put(Message msg) {
        try {
            queue.put(msg);
        } catch (InterruptedException e) {
            // do nothing
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
    public void commit() {
        // do nothing
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
