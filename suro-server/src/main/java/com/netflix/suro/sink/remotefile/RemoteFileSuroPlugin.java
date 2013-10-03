package com.netflix.suro.sink.remotefile;

import com.netflix.suro.SuroPlugin;
import com.netflix.suro.sink.remotefile.formatter.DateRegionStackFormatter;
import com.netflix.suro.sink.remotefile.formatter.SimpleDateFormatter;
import com.netflix.suro.sink.remotefile.formatter.StaticPrefixFormatter;

public class RemoteFileSuroPlugin extends SuroPlugin {
    @Override
    protected void configure() {
        this.addSinkType(S3FileSink.TYPE, S3FileSink.class);

        this.addRemotePrefixFormatterType(DateRegionStackFormatter.TYPE, DateRegionStackFormatter.class);
        this.addRemotePrefixFormatterType(SimpleDateFormatter.TYPE, SimpleDateFormatter.class);
        this.addRemotePrefixFormatterType(StaticPrefixFormatter.TYPE, StaticPrefixFormatter.class);
    }
}

