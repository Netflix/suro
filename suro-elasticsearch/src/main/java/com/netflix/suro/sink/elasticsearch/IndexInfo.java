package com.netflix.suro.sink.elasticsearch;

public interface IndexInfo {
    String getIndex();
    String getType();
    byte[] getSource();
    String getId();
    long getTimestamp();
}
