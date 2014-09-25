package com.netflix.suro.input;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.suro.message.MessageContainer;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface RecordParser {
    List<MessageContainer> parse(String data);
}
