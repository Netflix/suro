package com.netflix.suro;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.suro.queue.MessageSetProcessor;
import com.netflix.suro.server.StatusServer;
import com.netflix.suro.server.ThriftServer;
import com.netflix.suro.sink.SinkManager;
import org.apache.log4j.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Main service for suro to control all subsystems including
 * {@link StatusServer}, {@link ThriftServer}, {@link MessageSetProcessor}, and
 * {@link SinkManager}
 * 
 * @author elandau
 */
@Singleton
public class SuroService {
    static Logger log = Logger.getLogger(SuroServer.class);

    private final StatusServer statusServer;
    private final ThriftServer server;
    private final MessageSetProcessor queue;
    private final SinkManager  sinkManager;
    
    @Inject
    private SuroService(StatusServer statusServer, ThriftServer thriftServer, MessageSetProcessor queue, SinkManager sinkManager) {
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
            queue       .shutdown();
            sinkManager .shutdown();
        } catch (Exception e) {
            //ignore every exception while shutting down but loggign should be done for debugging
            log.error("Exception while shutting down SuroServer: " + e.getMessage(), e);
        }
    }
}
