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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.netflix.governator.guice.lazy.LazySingleton;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.TagKey;
import com.netflix.suro.input.SuroInput;
import com.netflix.suro.message.DefaultMessageContainer;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageSetBuilder;
import com.netflix.suro.message.MessageSetReader;
import com.netflix.suro.queue.Queue4Server;
import com.netflix.suro.routing.MessageRouter;
import com.netflix.suro.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The {@link TMessageSet} processor used by {@link com.netflix.suro.input.thrift.ThriftServer}. It takes incoming {@link TMessageSet}
 * sent by Suro client, validates each message set's CRC32 code, and then hands off validated message set to an internal queue.
 * A {@link MessageRouter} instance will asynchronously route the messages in the queue into configured sinks based on routing rules,
 * represented by {@link com.amazonaws.services.s3.model.RoutingRule}.
 *
 * Since this is the frontend of Thrift Server, it is implementing service status
 * and controlling to take the traffic or not.
 *
 * @author jbae
 */
@LazySingleton
public class MessageSetProcessor implements SuroServer.Iface {
    private static final Logger log = LoggerFactory.getLogger(MessageSetProcessor.class);

    private SuroInput input;
    private boolean isTakingTraffic = true;

    public void stopTakingTraffic(){
        this.isTakingTraffic = false;
    }
    public void startTakingTraffic(){
        this.isTakingTraffic = true;
    }

    @Override
    public ServiceStatus getStatus()  {
        if (isTakingTraffic){
            return ServiceStatus.ALIVE;
        } else {
            return ServiceStatus.WARNING;
        }
    }

    private volatile boolean isRunning = false;

    private final Queue4Server queue;
    private final MessageRouter router;
    private final ServerConfig config;
    private ExecutorService executors;
    private final ObjectMapper jsonMapper;
    
    @Inject
    public MessageSetProcessor(
        Queue4Server queue,
        MessageRouter router,
        ServerConfig config,
        ObjectMapper jsonMapper) throws Exception {
        this.queue = queue;
        this.router = router;
        this.config = config;
        this.jsonMapper = jsonMapper;

        isRunning = true;

        Monitors.registerObject(this);
    }

    private static final String messageCountMetrics = "messageCount";
    private static final String retryCountMetrics = "retryCount";
    private static final String dataCorruptionCountMetrics = "corruptedMessageCount";
    private static final String rejectedMessageCountMetrics = "rejectedMessageCount";
    private static final String messageProcessErrorMetrics = "processErrorCount";

    @Monitor(name ="QueueSize", type= DataSourceType.GAUGE)
    public int getQueueSize() {
        return queue.size();
    }

    @Override
    public String getName() throws TException {
        return "Suro-MessageQueue";
    }

    @Override
    public String getVersion() throws TException {
        return "V0.1.0";
    }

    @Override
    public Result process(TMessageSet messageSet) throws TException {
        Result result = new Result();
        try {
            // Stop adding chunks if it's no running
            if ( !isRunning) {
                DynamicCounter.increment(rejectedMessageCountMetrics,
                    TagKey.APP, messageSet.getApp(),
                    TagKey.REJECTED_REASON, "SURO_STOPPED");

                log.warn("Message processor is not running. Message rejected");
                result.setMessage("Suro server stopped");
                result.setResultCode(ResultCode.STOPPED);
                return result;
            }

            if ( !isTakingTraffic ) {
                DynamicCounter.increment(rejectedMessageCountMetrics,
                    TagKey.APP, messageSet.getApp(),
                    TagKey.REJECTED_REASON, "SURO_THROTTLING");

                log.warn("Suro is not taking traffic. Message rejected. ");
                result.setMessage("Suro server is not taking traffic");
                result.setResultCode(ResultCode.OTHER_ERROR);
                return result;
            }

            MessageSetReader reader = new MessageSetReader(messageSet);
            if (!reader.checkCRC()) {
                DynamicCounter.increment(dataCorruptionCountMetrics, TagKey.APP, messageSet.getApp());

                result.setMessage("data corrupted");
                result.setResultCode(ResultCode.CRC_CORRUPTED);
                return result;
            }

            if (queue.offer(messageSet)) {
                DynamicCounter.increment(
                    MonitorConfig.builder(messageCountMetrics)
                        .withTag(TagKey.APP, messageSet.getApp())
                        .build(),
                    messageSet.getNumMessages());

                result.setMessage(Long.toString(messageSet.getCrc()));
                result.setResultCode(ResultCode.OK);
            } else {
                DynamicCounter.increment(retryCountMetrics, TagKey.APP, messageSet.getApp());

                result.setMessage(Long.toString(messageSet.getCrc()));
                result.setResultCode(ResultCode.QUEUE_FULL);
            }

            return result;
        } catch (Exception e) {
            log.error("Exception when processing message set " + e.getMessage(), e);
        }

        return result;
    }

    public void start() {
        log.info("Starting processing message queue.");
        
        isRunning = true;

        executors = Executors.newFixedThreadPool(config.getMessageRouterThreads());

        for (int i = 0; i < config.getMessageRouterThreads(); ++i) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    TMessageSet tMessageSet;

                    long waitTime = config.messageRouterDefaultPollTimeout;

                    while (isRunning) {
                        try {
                            tMessageSet = queue.poll(waitTime, TimeUnit.MILLISECONDS);
                            if (tMessageSet == null) {
                                if (waitTime < config.messageRouterMaxPollTimeout) {
                                    waitTime += config.messageRouterDefaultPollTimeout;
                                }
                                continue;
                            }

                            waitTime = config.messageRouterDefaultPollTimeout;
                            processMessageSet(tMessageSet);
                        } catch (Exception e) {
                            log.error("Exception while handling TMessageSet: " + e.getMessage(), e);
                        }
                    }
                    // drain remain when shutting down
                    while ( !queue.isEmpty() ) {
                        try {
                            tMessageSet = queue.poll(0, TimeUnit.MILLISECONDS);
                            processMessageSet(tMessageSet);
                        } catch (Exception e) {
                            log.error("Exception while processing drained message set: "+e.getMessage(), e);
                        }
                    }
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    private void processMessageSet(TMessageSet tMessageSet) {
        MessageSetReader reader = new MessageSetReader(tMessageSet);

        for (final Message message : reader) {
            try {
                router.process(input, new DefaultMessageContainer(message, jsonMapper));
            } catch (Exception e) {
                DynamicCounter.increment(messageProcessErrorMetrics,
                    TagKey.APP, tMessageSet.getApp(),
                    TagKey.DATA_SOURCE, message.getRoutingKey());

                log.error(String.format("Failed to process message %s: %s", message, e.getMessage()), e);
            }
        }
    }

    @Override
    public long shutdown() throws TException {
        shutdown(config.messageRouterMaxPollTimeout * 2);
        return 0;
    }

    public void shutdown(long timeout) {
        log.info("MessageQueue is shutting down");
        isRunning = false;
        try {
            executors.shutdown();
            executors.awaitTermination(timeout, TimeUnit.MILLISECONDS);
            if ( !executors.isTerminated() ) {
                log.error("MessageDispatcher was not shut down gracefully");
            }
            executors.shutdownNow();
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    public TMessageSet poll(long timeout, TimeUnit unit) {
        try {
            return queue.poll(timeout, unit);
        } catch (InterruptedException e) {
            Thread.interrupted();
            return new MessageSetBuilder(new ClientConfig()).build();
        }
    }

    public void setInput(SuroInput input) {
        this.input = input;
    }
}
