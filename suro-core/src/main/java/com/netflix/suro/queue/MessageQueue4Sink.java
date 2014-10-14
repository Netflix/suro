package com.netflix.suro.queue;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.suro.message.Message;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Queue interface used in {@link com.netflix.suro.sink.Sink}.
 * With Java's {@code java.util.concurrent.BlockingQueue} interface, we cannot control 'commit' behavior such as
 * before writing messages retrieved from the queue to sink, when the process dies,
 * we will lose messages when messages are removed immediately after being polled.
 * To implement 'commit' behavior, we need to implement own queue interface.
 *
 * @author jbae
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = MemoryQueue4Sink.TYPE, value = MemoryQueue4Sink.class),
        @JsonSubTypes.Type(name = FileQueue4Sink.TYPE, value = FileQueue4Sink.class)
})
public interface MessageQueue4Sink {
    boolean offer(Message msg);
    Message poll(long timeout, TimeUnit timeUnit) throws InterruptedException;
    int drain(int batchSize, List<Message> msgList);
    void close();
    boolean isEmpty();
    long size();
    long remainingCapacity();
}
