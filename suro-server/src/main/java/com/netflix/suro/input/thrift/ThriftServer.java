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

package com.netflix.suro.input.thrift;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.governator.guice.lazy.LazySingleton;
import com.netflix.suro.input.SuroInput;
import com.netflix.suro.thrift.SuroServer;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

@LazySingleton
public class ThriftServer implements SuroInput {
    public static final String TYPE = "thrift";

    private static final Logger logger = LoggerFactory.getLogger(ThriftServer.class);

    private THsHaServer server = null;

    private final ServerConfig config;
    private final MessageSetProcessor msgProcessor;
    private ExecutorService executor;

    private int port;

    @JsonCreator
    public ThriftServer(
            @JacksonInject ServerConfig config,
            @JacksonInject MessageSetProcessor msgProcessor) throws Exception {
        this.config = config;
        this.msgProcessor = msgProcessor;
        this.msgProcessor.setInput(this);
    }

    @Override
    public String getId() {
        return TYPE;
    }

    @Override
    public void start() throws TTransportException {
        msgProcessor.start();

        logger.info("Starting ThriftServer with config " + config);
        CustomServerSocket transport = new CustomServerSocket(config);
        port = transport.getPort();
        SuroServer.Processor processor =  new SuroServer.Processor<MessageSetProcessor>(msgProcessor);

        THsHaServer.Args serverArgs = new THsHaServer.Args(transport);
        serverArgs.workerThreads(config.getThriftWorkerThreadNum());
        serverArgs.processor(processor);
        serverArgs.maxReadBufferBytes = config.getThriftMaxReadBufferBytes();

        executor = Executors.newSingleThreadExecutor();

        server = new THsHaServer(serverArgs);
        Future<?> serverStarted = executor.submit(new Runnable() {
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
        }  catch (TimeoutException e) {
            if (server.isServing()) {
                logger.info("Server started on port:" + config.getPort());
            } else {
                logger.error("ThriftServer didn't start up within: " + config.getStartupTimeout());
                Throwables.propagate(e);
            }
        } catch (ExecutionException e) {
            logger.error("Exception on starting ThriftServer: " + e.getMessage(), e);
            Throwables.propagate(e);
        }
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down thrift server");
        try {
            msgProcessor.stopTakingTraffic();
            Thread.sleep(1000);
            server.stop();
            executor.shutdownNow();
            msgProcessor.shutdown();
        } catch (Exception e) {
            // ignore any exception when shutdown
            logger.error("Exception while shutting down: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ThriftServer) {
            return true; // thrift server is singleton
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return TYPE.hashCode();
    }

    private ExecutorService pauseExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("ThriftServer-PauseExec-%d").build());

    @Override
    public void setPause(final long ms) {
        if (ms > 0) {
            pauseExecutor.execute(new Runnable() {

                @Override
                public void run() {
                    msgProcessor.stopTakingTraffic();
                    try {
                        Thread.sleep(ms);
                    } catch (InterruptedException e) {
                        // do nothing
                    }
                    msgProcessor.startTakingTraffic();
                }
            });
        }
    }

    // for testing purpose
    public int getPort() {
        return port;
    }
}