package com.netflix.suro.nofify;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class QueueNotify implements Notify {
    public static final String TYPE = "queue";

    private static final int DEFAULT_LENGTH = 100;
    private static final long DEFAULT_TIMEOUT = 1000;

    private final BlockingQueue<String> queue;
    private final long timeout;

    public QueueNotify() {
        queue = new LinkedBlockingQueue<String>(DEFAULT_LENGTH);
        timeout = DEFAULT_TIMEOUT;
    }

    @JsonCreator
    public QueueNotify(
            @JsonProperty("length") int length,
            @JsonProperty("recvTimeout") long timeout) {
        this.queue = new LinkedBlockingQueue<String>(length > 0 ? length : DEFAULT_LENGTH);
        this.timeout = timeout > 0 ? timeout : DEFAULT_TIMEOUT;
    }

    @Override
    public boolean send(String message) {
        return queue.offer(message);
    }

    @Override
    public String recv() {
        try {
            return queue.poll(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }
}
