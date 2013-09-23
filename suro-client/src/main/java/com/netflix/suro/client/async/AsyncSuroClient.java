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

package com.netflix.suro.client.async;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.netflix.config.DynamicIntProperty;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.TagKey;
import com.netflix.suro.client.ISuroClient;
import com.netflix.suro.client.SyncSuroClient;
import com.netflix.suro.connection.ConnectionPool;
import com.netflix.suro.message.Compression;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageSetBuilder;
import com.netflix.suro.message.MessageSetReader;
import com.netflix.suro.thrift.TMessageSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncSuroClient implements ISuroClient {
    static Logger log = LoggerFactory.getLogger(AsyncSuroClient.class);

    private final ClientConfig config;
    private final ConnectionPool connectionPool;
    private final BlockingQueue<Message> messageQueue;

    private final BlockingQueue<Runnable> jobQueue;
    private final ThreadPoolExecutor senders;
    private final MessageSetBuilder builder;

    @Monitor(name = TagKey.LOST_COUNT, type = DataSourceType.COUNTER)
    private AtomicLong lostMessages = new AtomicLong(0);
    @Override
    public long getLostMessageCount() {
        return lostMessages.get();
    }
    @Monitor(name = TagKey.SENT_COUNT, type = DataSourceType.COUNTER)
    private AtomicLong sentMessages = new AtomicLong(0);
    @Override
    public long getSentMessageCount() {
        return sentMessages.get();
    }
    @Monitor(name = TagKey.RESTORED_COUNT, type = DataSourceType.COUNTER)
    private AtomicLong restoredMessages = new AtomicLong(0);
    public long getRestoredMessageCount() {
        return restoredMessages.get();
    }
    @Monitor(name = TagKey.RETRIED_COUNT, type = DataSourceType.COUNTER)
    private AtomicLong retriedCount = new AtomicLong(0);
    public long getRetriedCount() {
        return retriedCount.get();
    }

    private ExecutorService poller = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("AsyncSuroClientPoller-%d").build());

    public static final String asyncRateLimitConfig = "SuroClient.asyncRateLimit";
    private final DynamicIntProperty rateLimitMsgPerSec = new DynamicIntProperty(asyncRateLimitConfig, 1000) {
        @Override
        protected void propertyChanged() {
            rateLimiter.setMsgPerSec(get());
        }
    };
    private final RateLimiter rateLimiter;

    @Inject
    public AsyncSuroClient(
            ClientConfig config,
            BlockingQueue<Message> messageQueue,
            ConnectionPool connectionPool) {
        this.config = config;
        this.messageQueue = messageQueue;

        this.connectionPool = connectionPool;
        this.builder = new MessageSetBuilder(config)
                .withCompression(Compression.create(config.getCompression()));

        poller.execute(createPoller());

        jobQueue = new ArrayBlockingQueue<Runnable>(config.getAsyncJobQueueCapacity());

        senders = new ThreadPoolExecutor(
                config.getAsyncSenderThreads(), config.getAsyncSenderThreads(),
                10, TimeUnit.SECONDS,
                jobQueue,
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        TMessageSet messageSet = ((AsyncSuroSender) r).getMessageSet();
                        for (Message m : new MessageSetReader(messageSet)) {
                            restore(m);
                        }
                    }
                });

        rateLimiter = new RateLimiter(rateLimitMsgPerSec.get());
        Monitors.registerObject(this);
    }

    @Override
    public void send(Message message) {
        if (messageQueue.offer(message) == false) {
            lostMessages.incrementAndGet();
            DynamicCounter.increment(
                    MonitorConfig.builder(TagKey.LOST_COUNT)
                            .withTag(TagKey.APP, config.getApp())
                            .withTag(TagKey.DATA_SOURCE, message.getRoutingKey())
                            .build());
        }
    }

    public void restore(Message message) {
        restoredMessages.incrementAndGet();
        DynamicCounter.increment(
                MonitorConfig.builder(TagKey.RESTORED_COUNT)
                        .withTag(TagKey.APP, config.getApp())
                        .withTag(TagKey.DATA_SOURCE, message.getRoutingKey())
                        .build());
        send(message);
    }

    private boolean running;


    private long lastBatch;
    private Runnable createPoller() {
        running = true;
        final AsyncSuroClient client = this;

        return new Runnable() {
            @Override
            public void run() {
                while (running || messageQueue.isEmpty() == false) {
                    try {
                        Message msg = messageQueue.poll(
                                Math.max(0, lastBatch + config.getAsyncTimeout() - System.currentTimeMillis()),
                                TimeUnit.MILLISECONDS);

                        boolean expired = (msg == null);
                        if (expired == false) {
                            builder.withMessage(msg.getRoutingKey(), msg.getPayload());
                            builder.drainFrom(messageQueue, config.getAsyncBatchSize() - builder.size());
                        }

                        boolean full = (builder.size() >= config.getAsyncBatchSize());
                        if ((expired || full) && builder.size() > 0) {
                            lastBatch = System.currentTimeMillis();
                            rateLimiter.pause(builder.size());
                            senders.execute(new AsyncSuroSender(builder.build(), client, config));
                        } else if (builder.size() == 0) {
                            Thread.sleep(config.getAsyncTimeout());
                        }
                    } catch (Exception e) {
                        log.error("MessageConsumer poller exception: " + e.getMessage(), e);
                    }
                }

                builder.drainFrom(messageQueue, messageQueue.size());
                if (builder.size() > 0) {
                    senders.execute(new AsyncSuroSender(builder.build(), client, config));
                }
            }
        };
    }

    @PreDestroy
    public void shutdown() {
        running = false;
        poller.shutdown();
        try {
            poller.awaitTermination(5000 + config.getAsyncTimeout(), TimeUnit.MILLISECONDS);
            senders.shutdown();
            senders.awaitTermination(5000 + config.getAsyncTimeout(), TimeUnit.MILLISECONDS);
            if (senders.isTerminated() == false) {
                log.error("AsyncSuroClient didn't terminate gracefully within 5 seconds");
                senders.shutdownNow();
            }
        } catch (InterruptedException e) {
            // ignore exceptions while shutting down
        }
    }

    @Monitor(name = "MessageQueueSize", type = DataSourceType.GAUGE)
    private int getMessageQueueSize() {
        return messageQueue.size();
    }

    @Monitor(name = "JobQueueSize", type = DataSourceType.GAUGE)
    private int getJobQueueSize() {
        return jobQueue.size();
    }

    @Monitor(name = "sendTime", type = DataSourceType.GAUGE)
    private long sendTime;
    public void updateSendTime(long sendTime) {
        this.sendTime = sendTime;
    }

    public void updateSentDataStats(TMessageSet messageSet, boolean retried) {
        sentMessages.addAndGet(
                SyncSuroClient.incrementMessageCount(
                        TagKey.SENT_COUNT,
                        config.getApp(),
                        new MessageSetReader(messageSet)));
        if (retried) {
            retriedCount.incrementAndGet();
        }
    }

    public ConnectionPool getConnectionPool() {
        return connectionPool;
    }

    @Monitor(name = "senderExceptionCount", type = DataSourceType.COUNTER)
    private AtomicLong senderExceptionCount = new AtomicLong(0);
    public void updateSenderException() {
        senderExceptionCount.incrementAndGet();
    }
}