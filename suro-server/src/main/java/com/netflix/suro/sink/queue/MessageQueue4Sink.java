package com.netflix.suro.sink.queue;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.suro.message.Message;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = MemoryQueue4Sink.TYPE, value = MemoryQueue4Sink.class),
        @JsonSubTypes.Type(name = FileQueue4Sink.TYPE, value = FileQueue4Sink.class)
})
public interface MessageQueue4Sink {
    void put(Message msg);
    int retrieve(int batchSize, List<Message> msgList);
    void commit();
    void close();
    boolean isEmpty();
    long size();
}
