package com.netflix.suro.server;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.suro.sink.SinkManager;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Singleton
@Path("/sinkstat")
public class SinkStat {
    private final SinkManager sinkManager;

    @Inject
    public SinkStat(SinkManager sinkManager) {
        this.sinkManager = sinkManager;
    }

    @GET
    @Produces("text/plain")
    public String get() {
        return sinkManager.reportSinkStat();
    }
}
