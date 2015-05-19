package com.netflix.suro.queue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.suro.message.Message;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * Memory based {@link MessageQueue4Sink}
 * <p/>
 * If capacity is not zero, it delegates to {@link ArrayBlockingQueue}. The queue is typically used as an asynchronous
 * buffer between the input and sink where adding messages to the queue will fail if queue is full. Otherwise when
 * capacity is zero, it delegates to {@link SynchronousQueue}, where the there is no buffer and input and sink are always
 * synchronized. This is helpful when the sink does not want to deal with back-pressure but want to slow down the
 * input without dropping messages.
 *
 * @author jbae
 */
@NotThreadSafe
public class MemoryQueue4Sink implements MessageQueue4Sink {
    public static final String TYPE = "memory";

    private final BlockingQueue<Message> queue;
    private final int capacity;

    @JsonCreator
    public MemoryQueue4Sink(@JsonProperty("capacity") int capacity) {
        this.capacity = capacity;
        if (capacity == 0) {
            this.queue = new SynchronousQueue();
        } else {
            this.queue = new ArrayBlockingQueue(capacity);
        }
    }

    @Override
    public boolean offer(Message msg) {
        if (capacity == 0) {
            try {
                // for synchronous queue, this will block until the sink takes the message from the queue
                queue.put(msg);
            } catch (InterruptedException e) {
                return false;
            }
            return true;
        } else {
            return queue.offer(msg);
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

    @Override
    public long remainingCapacity() {
        return queue.remainingCapacity();
    }
}
