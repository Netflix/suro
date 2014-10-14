package com.netflix.suro.input;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface SuroInput {
    String getId();
    void start() throws Exception;
    void shutdown();

    void setPause(long ms);
}
