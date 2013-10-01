package com.netflix.suro.queue;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.suro.message.Message;

import java.util.List;
import java.util.concurrent.TimeUnit;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = MemoryQueue4Sink.TYPE, value = MemoryQueue4Sink.class),
        @JsonSubTypes.Type(name = FileQueue4Sink.TYPE, value = FileQueue4Sink.class)
})
public interface MessageQueue4Sink {
    boolean offer(Message msg);
    Message poll(long timeout, TimeUnit timeUnit) throws InterruptedException;
    int drain(int batchSize, List<Message> msgList);
    void commit();
    void close();
    boolean isEmpty();
    long size();
}
