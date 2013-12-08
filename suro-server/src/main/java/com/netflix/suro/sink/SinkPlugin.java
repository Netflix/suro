package com.netflix.suro.sink;

import com.netflix.suro.SuroPlugin;
import com.netflix.suro.sink.notification.NoNotification;
import com.netflix.suro.sink.notification.QueueNotification;
import com.netflix.suro.sink.notification.SQSNotification;
import com.netflix.suro.sink.kafka.KafkaSink;
import com.netflix.suro.sink.localfile.LocalFileSink;
import com.netflix.suro.sink.remotefile.S3FileSink;
import com.netflix.suro.sink.remotefile.formatter.DateRegionStackFormatter;
import com.netflix.suro.sink.remotefile.formatter.SimpleDateFormatter;
import com.netflix.suro.sink.remotefile.formatter.StaticPrefixFormatter;

/**
 *
 * @author jbae
 */
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

        this.addNotificationType(NoNotification.TYPE, NoNotification.class);
        this.addNotificationType(QueueNotification.TYPE, QueueNotification.class);
        this.addNotificationType(SQSNotification.TYPE, SQSNotification.class);
    }
}
