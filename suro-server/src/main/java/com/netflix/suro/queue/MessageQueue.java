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

package com.netflix.suro.queue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.governator.guice.lazy.LazySingleton;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.TagKey;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.message.MessageSetBuilder;
import com.netflix.suro.message.MessageSetReader;
import com.netflix.suro.routing.MessageRouter;
import com.netflix.suro.server.ServerConfig;
import com.netflix.suro.thrift.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is actual worker for Thrift Server Processor
 * Simply, it's accepting TMessageSet sent from suro client, checking CRC
 * and adding it to the queue. Internally, it also implements message routing
 * threads to read Message from TMessageSet and route them to the sinks.
 *
 * Since this is the frontend of Thrift Server, it is implementing service status
 * and controlling to take the traffic or not.
 *
 * @author jbae
 */
@LazySingleton
public class MessageQueue implements SuroServer.Iface {
    static Logger log = LoggerFactory.getLogger(MessageQueue.class);
    
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
    private final ObjectMapper mapper;
    
    @Inject
    public MessageQueue(
            Queue4Server queue,
            MessageRouter router,
            QueueManager manager,
            ServerConfig config, 
            ObjectMapper mapper) throws Exception {
        this.queue = queue;
        this.router = router;
        this.config = config;
        this.mapper = mapper;

        isRunning = true;

        manager.registerService(this);
        Monitors.registerObject(this);
    }

    @Monitor(name ="QueueSize", type= DataSourceType.GAUGE)
    public int getQueueSize() {
        return (int) queue.size();
    }

    private static final String messageCountMetric = "messageCount";
    @Monitor(name= messageCountMetric, type=DataSourceType.COUNTER)
    private AtomicLong messageCount = new AtomicLong();

    private static final String retryCountMetric = "retryCount";
    @Monitor(name=retryCountMetric, type=DataSourceType.COUNTER)
    private AtomicLong retryCount = new AtomicLong();

    private static final String dataCorruptionCountMetric = "dataCorruptionCount";
    @Monitor(name=dataCorruptionCountMetric, type=DataSourceType.COUNTER)
    private AtomicLong dataCorruption = new AtomicLong();

    private static final String messageProcessErrorMetric = "processErrorCount";
    @Monitor(name=dataCorruptionCountMetric, type=DataSourceType.COUNTER)
    private AtomicLong processErrorCount = new AtomicLong();

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
            if (isRunning == false) {
                log.warn("Rejecting some incoming trafic!");
                result.setMessage("Shutting down");
                result.setResultCode(ResultCode.STOPPED);
                return result;
            }

            if (isTakingTraffic == false) {
                log.warn("Rejecting some incoming trafic! - >>>>>>> Flag is ON <<<<<<< ");
                result.setMessage("collector in error");
                result.setResultCode(ResultCode.OTHER_ERROR);
                return result;
            }

            MessageSetReader reader = new MessageSetReader(messageSet);
            if (reader.checkCRC() == false) {
                dataCorruption.incrementAndGet();

                DynamicCounter.increment(dataCorruptionCountMetric, TagKey.APP, messageSet.getApp());

                result.setMessage("data corrupted");
                result.setResultCode(ResultCode.CRC_CORRUPTED);
                return result;
            }

            if (queue.offer(messageSet)) {
                messageCount.incrementAndGet();

                DynamicCounter.increment(
                        MonitorConfig.builder(messageCountMetric)
                                .withTag(TagKey.APP, messageSet.getApp())
                                .build(), messageSet.getNumMessages());

                result.setMessage(Long.toString(messageSet.getCrc()));
                result.setResultCode(ResultCode.OK);
            } else {
                retryCount.incrementAndGet();

                DynamicCounter.increment(retryCountMetric, TagKey.APP, messageSet.getApp());

                result.setMessage(Long.toString(messageSet.getCrc()));
                result.setResultCode(ResultCode.QUEUE_FULL);
            }

            return result;
        } catch (Throwable e) {
            log.error("Throable handled: " + e.getMessage(), e);
        }

        return result;
    }

    public void start() {
        log.info("MessageQueue starting");
        
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
                    // drain remains when shutdown
                    while (queue.isEmpty() == false) {
                        try {
                            tMessageSet = queue.poll(0, TimeUnit.MILLISECONDS);
                            processMessageSet(tMessageSet);
                        } catch (Exception e) {
                            log.error("Exception while handling TMessageSet");
                        }
                    }
                }
            });
        }
    }

    private void processMessageSet(TMessageSet tMessageSet) {
        MessageSetReader reader = new MessageSetReader(tMessageSet);

        for (final Message message : reader) {
            try {
                router.process(new MessageContainer() {
                    class Item {
                        TypeReference<?> tr;
                        Object obj;
                    }
                    
                    private String       str;
                    private List<Item> cache;
                    
                    @Override
                    public <T> T getEntity(Class<T> clazz) throws Exception {
                        if (clazz.equals(byte[].class))
                            return (T)message.getPayload();
                        else if (clazz.equals(String.class)) {
                            return (T)new String(message.getPayload());
                        }
                        else {
                            TypeReference<T> typeReference = new TypeReference<T>(){};
                            if (cache == null) {
                                cache = Lists.newLinkedList();
                            }
                            for (Item item : cache) {
                                if (item.tr.equals(typeReference))
                                    return (T)item.obj;
                            }
                            Item item = new Item();
                            item.tr = typeReference;
                            item.obj = mapper.readValue(message.getPayload(), typeReference);
                            cache.add(item);
                            return (T)item.obj;
                        }
                    }
    
                    @Override
                    public String getRoutingKey() {
                        return message.getRoutingKey();
                    }
    
                    @Override
                    public Message getMessage() {
                        return message;
                    }
                    
                });
            }
            catch (Exception e) {
                processErrorCount.incrementAndGet();
            }
        }
    }

    @Override
    public long shutdown() throws TException {
        log.info("MessageQueue is shutting down");
        isRunning = false;
        try {
            executors.shutdown();
            executors.awaitTermination(config.messageRouterMaxPollTimeout * 2, TimeUnit.MILLISECONDS);
            if (executors.isTerminated() == false) {
                log.error("MessageDispatcher was not shutdown gracefully");
            }
            executors.shutdownNow();
        } catch (InterruptedException e) {
            // ignore exceptions while shutting down
        }
        return 0;
    }

    public TMessageSet poll(long timeout, TimeUnit unit) {
        try {
            return queue.poll(timeout, unit);
        } catch (InterruptedException e) {
            // return empty payload
            return new MessageSetBuilder(new ClientConfig()).build();
        }
    }
}
