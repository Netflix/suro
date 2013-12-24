package com.netflix.suro.sink;

import com.netflix.suro.SuroPlugin;
import com.netflix.suro.sink.kafka.KafkaSink;
import com.netflix.suro.sink.notice.NoNotice;

/**
 *
 * @author jbae
 */
public class ClientSinkPlugin extends SuroPlugin {
    @Override
    protected void configure() {
        this.addSinkType(SuroSink.TYPE, SuroSink.class);
        this.addSinkType(KafkaSink.TYPE, KafkaSink.class);

        this.addNoticeType(NoNotice.TYPE, NoNotice.class);
    }
}
