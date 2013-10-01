package com.netflix.suro.sink;

public class SuroSinkPlugin extends SuroPlugin {
    @Override
    protected void configure() {
        this.addSinkType(SuroSink.TYPE, SuroSink.class);
    }
}
