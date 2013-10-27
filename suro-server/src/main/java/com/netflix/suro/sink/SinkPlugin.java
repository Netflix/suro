package com.netflix.suro.sink;

import com.netflix.suro.SuroPlugin;
import com.netflix.suro.sink.nofify.NoNotify;
import com.netflix.suro.sink.nofify.QueueNotify;
import com.netflix.suro.sink.nofify.SQSNotify;
import com.netflix.suro.sink.kafka.KafkaSink;
import com.netflix.suro.sink.localfile.LocalFileSink;
import com.netflix.suro.sink.remotefile.S3FileSink;
import com.netflix.suro.sink.remotefile.formatter.DateRegionStackFormatter;
import com.netflix.suro.sink.remotefile.formatter.SimpleDateFormatter;
import com.netflix.suro.sink.remotefile.formatter.StaticPrefixFormatter;

public class SinkPlugin extends SuroPlugin {
    @Override
    protected void configure() {
        this.addSinkType(LocalFileSink.TYPE, LocalFileSink.class);

        this.addSinkType(KafkaSink.TYPE, KafkaSink.class);

        this.addSinkType(S3FileSink.TYPE, S3FileSink.class);
        this.addRemotePrefixFormatterType(DateRegionStackFormatter.TYPE, DateRegionStackFormatter.class);
        this.addRemotePrefixFormatterType(SimpleDateFormatter.TYPE, SimpleDateFormatter.class);
        this.addRemotePrefixFormatterType(StaticPrefixFormatter.TYPE, StaticPrefixFormatter.class);

        this.addSinkType(SuroSink.TYPE, SuroSink.class);

        this.addNotifyType(NoNotify.TYPE, NoNotify.class);
        this.addNotifyType(QueueNotify.TYPE, QueueNotify.class);
        this.addNotifyType(SQSNotify.TYPE, SQSNotify.class);
    }
}
