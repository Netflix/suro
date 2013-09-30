package com.netflix.suro.sink;

import com.netflix.suro.SuroPlugin;

public class SuroSinkPlugin extends SuroPlugin {
    @Override
    protected void configure() {
        this.addSinkType(SuroSink.TYPE, SuroSink.class);
    }
}
