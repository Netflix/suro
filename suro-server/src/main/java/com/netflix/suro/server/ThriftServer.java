/*
 * Copyright 2013 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.suro.server;

import com.google.inject.Inject;
import com.netflix.governator.guice.lazy.LazySingleton;
import com.netflix.suro.queue.MessageQueue;
import com.netflix.suro.thrift.SuroServer;
import org.apache.thrift.TException;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

@LazySingleton
public class ThriftServer {
    private static Logger logger = LoggerFactory.getLogger(ThriftServer.class);

    private CustomServerSocket transport = null;
    private THsHaServer server = null;
    private SuroServer.Processor processor = null;

    private final ServerConfig config;
    private final MessageQueue messageQueue;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Inject
    public ThriftServer(
            ServerConfig config,
            MessageQueue messageQueue) throws Exception {
        this.config = config;
        this.messageQueue = messageQueue;
    }

    private Future serverStarted;
    
    public void start() throws TTransportException {
        logger.info("Starting ThriftServer with config " + config);
        transport = new CustomServerSocket(config);
        processor =  new SuroServer.Processor(messageQueue);

        THsHaServer.Args serverArgs = new THsHaServer.Args(transport);
        serverArgs.workerThreads(config.getThriftWorkerThreadNum());
        serverArgs.processor(processor);
        serverArgs.maxReadBufferBytes = config.getThriftMaxReadBufferBytes();

        server = new THsHaServer(serverArgs);
        serverStarted = executor.submit(new Runnable() {
            @Override
            public void run() {
                server.serve();
            }
        });
        try {
            serverStarted.get(config.getStartupTimeout(), TimeUnit.MILLISECONDS);
            if (server.isServing()) {
                logger.info("Server started on port:" + config.getPort());
            } else {
                throw new RuntimeException("ThriftServer didn't start up within: " + config.getStartupTimeout());
            }
        } catch (InterruptedException e) {
            // ignore this type of exception
        } catch (ExecutionException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (TimeoutException e) {
            if (server.isServing()) {
                logger.info("Server started on port:" + config.getPort());
            } else {
                logger.error("ThriftServer didn't start up within: " + config.getStartupTimeout());
                System.exit(-1);
            }
        }

    }

    public boolean isServing(){
        return server != null && server.isServing();
    }

    public boolean isStopped(){
        return server == null || server.isStopped();
    }

    public void join() {
        try {
            // wait forever until shutdown() called
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // ignore exception
        }
    }

    public void shutdown() {
        logger.info("Shutting down thrift server");
        try {
            server.stop();
            executor.shutdownNow();
        } catch (Exception e) {
            // ignore any exception when shutdown
            logger.error("Exception while shutting down: " + e.getMessage(), e);
        }
    }
}