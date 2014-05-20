package com.netflix.suro.sink.remotefile;

import com.netflix.suro.SuroPlugin;
import com.netflix.suro.message.Message;
import com.netflix.suro.sink.SuroSink;
import com.netflix.suro.sink.localfile.LocalFileSink;
import com.netflix.suro.sink.notice.NoNotice;
import com.netflix.suro.sink.notice.QueueNotice;
import com.netflix.suro.sink.remotefile.formatter.DateRegionStackFormatter;
import com.netflix.suro.sink.remotefile.formatter.SimpleDateFormatter;
import com.netflix.suro.sink.remotefile.formatter.StaticPrefixFormatter;

public class SuroSinkPlugin extends SuroPlugin {
    @Override
    protected void configure() {
        this.addSinkType(LocalFileSink.TYPE, LocalFileSink.class);


        this.addSinkType(S3FileSink.TYPE, S3FileSink.class);
        this.addSinkType(HdfsFileSink.TYPE, HdfsFileSink.class);
        this.addRemotePrefixFormatterType(DateRegionStackFormatter.TYPE, DateRegionStackFormatter.class);
        this.addRemotePrefixFormatterType(SimpleDateFormatter.TYPE, SimpleDateFormatter.class);
        this.addRemotePrefixFormatterType(StaticPrefixFormatter.TYPE, StaticPrefixFormatter.class);

        this.addSinkType(SuroSink.TYPE, SuroSink.class);

        this.addNoticeType(NoNotice.TYPE, NoNotice.class);
        this.addNoticeType(QueueNotice.TYPE, QueueNotice.class);
    }
}

