package com.netflix.suro.sink;

import com.netflix.suro.SuroPlugin;
import com.netflix.suro.message.Message;
import com.netflix.suro.sink.elasticsearch.ElasticSearchSink;
import com.netflix.suro.sink.kafka.KafkaSink;
import com.netflix.suro.sink.kafka.KafkaSinkV2;
import com.netflix.suro.sink.kafka.SuroKeyedMessage;
import com.netflix.suro.sink.localfile.LocalFileSink;
import com.netflix.suro.sink.notice.LogNotice;
import com.netflix.suro.sink.notice.NoNotice;
import com.netflix.suro.sink.notice.QueueNotice;
import com.netflix.suro.sink.notice.SQSNotice;
import com.netflix.suro.sink.remotefile.HdfsFileSink;
import com.netflix.suro.sink.remotefile.S3FileSink;
import com.netflix.suro.sink.remotefile.formatter.DateRegionStackFormatter;
import com.netflix.suro.sink.remotefile.formatter.DynamicRemotePrefixFormatter;

/**
 *
 * @author jbae
 */
public class ServerSinkPlugin extends SuroPlugin {
    @Override
    protected void configure() {
        this.addSinkType(LocalFileSink.TYPE, LocalFileSink.class);

        this.addSinkType(ElasticSearchSink.TYPE, ElasticSearchSink.class);

        this.addSinkType(KafkaSink.TYPE, KafkaSink.class);
        this.addSinkType(KafkaSinkV2.TYPE, KafkaSinkV2.class);
        Message.classMap.put((byte) 1, SuroKeyedMessage.class);

        this.addSinkType(S3FileSink.TYPE, S3FileSink.class);
        this.addSinkType(HdfsFileSink.TYPE, HdfsFileSink.class);
        this.addRemotePrefixFormatterType(DateRegionStackFormatter.TYPE, DateRegionStackFormatter.class);
        this.addRemotePrefixFormatterType(DynamicRemotePrefixFormatter.TYPE, DynamicRemotePrefixFormatter.class);

        this.addSinkType(SuroSink.TYPE, SuroSink.class);

        this.addNoticeType(NoNotice.TYPE, NoNotice.class);
        this.addNoticeType(QueueNotice.TYPE, QueueNotice.class);
        this.addNoticeType(SQSNotice.TYPE, SQSNotice.class);
        this.addNoticeType(LogNotice.TYPE, LogNotice.class);
    }
}
