package com.netflix.suro.sink.remotefile;

import com.netflix.suro.SuroPlugin;

public class RemoteFileSuroPlugin extends SuroPlugin {
    @Override
    protected void configure() {
        this.addSinkType(S3FileSink.TYPE, S3FileSink.class);
    }
}

