package com.netflix.suro.sink.sqs;

import com.netflix.suro.sink.SuroPlugin;

public class SqsSinkSuroPlugin extends SuroPlugin {
    @Override
    protected void configure() {
        this.addSinkType(SqsSink.TYPE, SqsSink.class);
    }
}
