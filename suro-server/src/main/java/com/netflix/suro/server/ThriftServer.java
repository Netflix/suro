package com.netflix.suro.server;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.suro.queue.MessageQueue;
import com.netflix.suro.thrift.SuroServer;
import org.apache.thrift.TException;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Singleton
public class ThriftServer {
    private static Logger logger = LoggerFactory.getLogger(ThriftServer.class);

    private CustomServerSocket transport = null;
    private THsHaServer server = null;
    private SuroServer.Processor processor = null;

    private final ServerConfig config;
    private final MessageQueue messageQueue;

    @Inject
    public ThriftServer(ServerConfig config, MessageQueue messageQueue) throws Exception {
        this.config = config;
        this.messageQueue = messageQueue;
    }

    @PostConstruct
    public void start() throws TTransportException {
        transport = new CustomServerSocket(config);
        processor =  new SuroServer.Processor(messageQueue);

        THsHaServer.Args serverArgs = new THsHaServer.Args(transport);
        serverArgs.workerThreads(config.getThriftWorkerThreadNum());
        serverArgs.processor(processor);
        serverArgs.maxReadBufferBytes = config.getThriftMaxReadBufferBytes();

        server = new THsHaServer(serverArgs);
        logger.info("Server started on port:" + config.getPort());
        server.serve();
    }

    public boolean isServing(){
        return server != null && server.isServing();
    }

    public boolean isStopped(){
        return server == null || server.isStopped();
    }

    @PreDestroy
    public void shutdown() {
        try {
            messageQueue.shutdown();
        } catch (TException e) {
            // ignore any exception when shutdown
        }
    }
}
