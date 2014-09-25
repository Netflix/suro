package com.netflix.suro.input;

import com.netflix.suro.SuroPlugin;
import com.netflix.suro.input.kafka.KafkaConsumer;
import com.netflix.suro.input.remotefile.CloudTrail;
import com.netflix.suro.input.remotefile.JsonLine;
import com.netflix.suro.input.remotefile.S3Consumer;
import com.netflix.suro.input.thrift.ThriftServer;

public class SuroInputPlugin extends SuroPlugin {
    @Override
    protected void configure() {
        this.addInputType(ThriftServer.TYPE, ThriftServer.class);
        this.addInputType(KafkaConsumer.TYPE, KafkaConsumer.class);
        this.addInputType(S3Consumer.TYPE, S3Consumer.class);

        this.addRecordParserType(JsonLine.TYPE, JsonLine.class);
        this.addRecordParserType(CloudTrail.TYPE, CloudTrail.class);
    }
}
