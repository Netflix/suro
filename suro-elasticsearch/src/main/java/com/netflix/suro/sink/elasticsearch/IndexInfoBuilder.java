package com.netflix.suro.sink.elasticsearch;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.suro.message.Message;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "default", value = DefaultIndexInfoBuilder.class)
})
public interface IndexInfoBuilder {
    IndexInfo create(Message msg);
    String getActionMetadata(IndexInfo info);
    String getSource(IndexInfo info) throws Exception;
    String getIndexUri(IndexInfo info);
    String getCommand();
}
