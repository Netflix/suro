package com.netflix.suro.sink.kafka;

import com.netflix.suro.SuroPlugin;

public class KafkaSinkPlugin extends SuroPlugin {
    @Override
    protected void configure() {
        this.addSinkType(KafkaSink.TYPE, KafkaSink.class);
    }
}
