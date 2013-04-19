package com.netflix.suro.sink.localfile;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.serde.SerDe;

import java.io.IOException;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = SequenceFileWriter.TYPE, value = SequenceFileWriter.class),
        @JsonSubTypes.Type(name = TextFileWriter.TYPE, value = TextFileWriter.class)
})
public interface FileWriter {
    void open(String outputDir) throws IOException;
    long getLength() throws IOException;
    void writeTo(Message message, SerDe serde) throws IOException;
    void rotate(String newPath) throws IOException;

    void close() throws IOException;
    void setDone(String oldName, String newName) throws IOException;
}
