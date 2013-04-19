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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Singleton
public class ThriftServer {
    private static Logger logger = LoggerFactory.getLogger(ThriftServer.class);

    private CustomServerSocket transport = null;
    private THsHaServer server = null;
    private SuroServer.Processor processor = null;

    private final ServerConfig config;
    private final MessageQueue messageQueue;
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    @Inject
    public ThriftServer(
            ServerConfig config,
            MessageQueue messageQueue) throws Exception {
        this.config = config;
        this.messageQueue = messageQueue;
    }

    public void start() throws TTransportException {
        transport = new CustomServerSocket(config);
        processor =  new SuroServer.Processor(messageQueue);

        THsHaServer.Args serverArgs = new THsHaServer.Args(transport);
        serverArgs.workerThreads(config.getThriftWorkerThreadNum());
        serverArgs.processor(processor);
        serverArgs.maxReadBufferBytes = config.getThriftMaxReadBufferBytes();

        server = new THsHaServer(serverArgs);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                server.serve();
            }
        });
        while (isServing() == false) {
            try { Thread.sleep(1000); } catch (InterruptedException e) {}
        }

        logger.info("Server started on port:" + config.getPort());
    }

    public boolean isServing(){
        return server != null && server.isServing();
    }

    public boolean isStopped(){
        return server == null || server.isStopped();
    }

    public void shutdown() {
        try {
            server.stop();
            executor.shutdownNow();
            messageQueue.shutdown();
        } catch (TException e) {
            // ignore any exception when shutdown
        }
    }
}