package com.netflix.suro.sink.localfile;

import com.netflix.suro.sink.SuroPlugin;

public class LocalFileSuroPlugin extends SuroPlugin {
    @Override
    protected void configure() {
        this.addSinkType(LocalFileSink.TYPE, LocalFileSink.class);
    }
}
