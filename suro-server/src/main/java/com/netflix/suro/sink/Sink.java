package com.netflix.suro.sink;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.serde.SerDe;
import com.netflix.suro.sink.localfile.LocalFileSink;
import com.netflix.suro.sink.remotefile.S3FileSink;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = LocalFileSink.TYPE, value = LocalFileSink.class),
        @JsonSubTypes.Type(name = S3FileSink.TYPE, value = S3FileSink.class)
})
public interface Sink {
    void writeTo(Message message, SerDe serde);

    void open();
    void close();
    String recvNotify();
    String getStat();
}
