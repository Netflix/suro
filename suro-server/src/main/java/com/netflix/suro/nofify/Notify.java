package com.netflix.suro.nofify;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = QueueNotify.TYPE, value = QueueNotify.class),
        @JsonSubTypes.Type(name = NoNotify.TYPE, value = QueueNotify.class),
        @JsonSubTypes.Type(name = SQSNotify.TYPE, value = SQSNotify.class)
})
public interface Notify {
    void init();
    boolean send(String message);
    String recv();
    String getStat();
}
