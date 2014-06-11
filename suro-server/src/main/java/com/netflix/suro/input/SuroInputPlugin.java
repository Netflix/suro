package com.netflix.suro.input;

import com.netflix.suro.SuroPlugin;
import com.netflix.suro.input.kafka.KafkaConsumer;
import com.netflix.suro.input.thrift.ThriftServer;

public class SuroInputPlugin extends SuroPlugin {
    @Override
    protected void configure() {
        this.addInputType(ThriftServer.TYPE, ThriftServer.class);
        this.addInputType(KafkaConsumer.TYPE, KafkaConsumer.class);
    }
}
