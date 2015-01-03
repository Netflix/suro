package com.netflix.suro.sink.elasticsearch;

public interface IndexInfo {
    String getIndex();
    String getType();
    Object getSource();
    String getId();
    long getTimestamp();
}
