package com.netflix.suro.sink.localfile;

import com.netflix.suro.SuroPlugin;
import com.netflix.suro.sink.SuroSink;
import com.netflix.suro.sink.notice.NoNotice;
import com.netflix.suro.sink.notice.QueueNotice;

public class SuroSinkPlugin extends SuroPlugin {
    @Override
    protected void configure() {
        this.addSinkType(LocalFileSink.TYPE, LocalFileSink.class);

        this.addSinkType(SuroSink.TYPE, SuroSink.class);

        this.addNoticeType(NoNotice.TYPE, NoNotice.class);
        this.addNoticeType(QueueNotice.TYPE, QueueNotice.class);
    }
}
