package com.netflix.suro;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.log4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.suro.queue.MessageQueue;
import com.netflix.suro.server.StatusServer;
import com.netflix.suro.server.ThriftServer;
import com.netflix.suro.sink.SinkManager;

/**
 * Main service for suro to control all subsystems
 * 
 * @author elandau
 */
@Singleton
public class SuroService {
    static Logger log = Logger.getLogger(SuroServer.class);

    private final StatusServer statusServer;
    private final ThriftServer server;
    private final MessageQueue queue;
    private final SinkManager  sinkManager;
    
    @Inject
    private SuroService(StatusServer statusServer, ThriftServer thriftServer, MessageQueue queue, SinkManager sinkManager) {
        this.statusServer = statusServer;
        this.server       = thriftServer;
        this.queue        = queue;
        this.sinkManager  = sinkManager;
    }

    @PostConstruct
    public void start() {
        try {
            queue.start();
            server.start();
            statusServer.start();
        } 
        catch (Exception e) {
            log.error("Exception while starting up server: " + e.getMessage(), e);
            System.exit(-1);
        }
    }

    @PreDestroy
    public void shutdown() {
        try {
            server      .shutdown();
            statusServer.shutdown();
            sinkManager .shutdown();
            queue       .shutdown();
        } catch (Exception e) {
            //ignore every exception while shutting down but loggign should be done for debugging
            log.error("Exception while shutting down SuroServer: " + e.getMessage(), e);
        }
    }

}
