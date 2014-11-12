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

import com.google.common.annotations.VisibleForTesting;
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
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Asynchronous {@link ISuroClient} implementation. This client puts messages into a queue first, and has a thread send
 * out events off the queue asynchronously.
 */
public class AsyncSuroClient implements ISuroClient {
    private static final Logger log = LoggerFactory.getLogger(AsyncSuroClient.class);

    private final ClientConfig config;
    private final ConnectionPool connectionPool;
    private final Queue4Client messageQueue;

    private final BlockingQueue<Runnable> jobQueue;
    private final ThreadPoolExecutor senders;
    private final MessageSetBuilder builder;

    @Monitor(name = TagKey.LOST_COUNT, type = DataSourceType.COUNTER)
    private AtomicLong lostMessages = new AtomicLong(0);
    @Override
    public long getLostMessageCount() {
        return lostMessages.get();
    }

    @Monitor(name = "MessageQueueSize", type = DataSourceType.GAUGE)
    @Override
    public long getNumOfPendingMessages() {
        return messageQueue.size();
    }

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
    private final DynamicIntProperty rateLimitMsgPerSec = new DynamicIntProperty(asyncRateLimitConfig, Integer.MAX_VALUE) {
        @Override
        protected void propertyChanged() {
            rateLimiter.setMsgPerSec(get());
        }
    };
    private final RateLimiter rateLimiter;

    @Inject
    public AsyncSuroClient(
            ClientConfig config,
            Queue4Client messageQueue,
            ConnectionPool connectionPool) {
        this.config = config;
        this.messageQueue = messageQueue;

        this.connectionPool = connectionPool;
        this.builder = new MessageSetBuilder(config)
                .withCompression(Compression.create(config.getCompression()));

        poller.execute(createPoller());

        jobQueue = new ArrayBlockingQueue<Runnable>(config.getAsyncJobQueueCapacity())
        {
            @Override
            public boolean offer(Runnable runnable) {
                try {
                    put(runnable); // not to reject the task, slowing down
                } catch (InterruptedException e) {
                    // do nothing
                }
                return true;
            }
        }
        ;

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
        if (!messageQueue.offer(message)) {
            lostMessages.incrementAndGet();
            DynamicCounter.increment(
                    MonitorConfig.builder(TagKey.LOST_COUNT)
                            .withTag(TagKey.APP, config.getApp())
                            .withTag(TagKey.DATA_SOURCE, message.getRoutingKey())
                            .build());
            for (Listener listener : listeners) {
                listener.lostCallback(1);
            }
        }
    }

    public void restore(Message message) {
        restoredMessages.incrementAndGet();
        DynamicCounter.increment(
                MonitorConfig.builder(TagKey.RESTORED_COUNT)
                        .withTag(TagKey.APP, config.getApp())
                        .withTag(TagKey.DATA_SOURCE, message.getRoutingKey())
                        .build());
        for (Listener listener : listeners) {
            listener.restoredCallback();
        }
        send(message);
    }

    @VisibleForTesting
    protected long queuedMessageSetCount = 0;

    private boolean running;

    private long lastBatch;
    private Runnable createPoller() {
        running = true;
        final AsyncSuroClient client = this;

        return new Runnable() {
            @Override
            public void run() {
                while (running || !messageQueue.isEmpty()) {
                    try {
                        Message msg = messageQueue.poll(
                                Math.max(0, lastBatch + config.getAsyncTimeout() - System.currentTimeMillis()),
                                TimeUnit.MILLISECONDS);

                        boolean expired = (msg == null);
                        if (!expired) {
                            builder.withMessage(msg.getRoutingKey(), msg.getPayload());
                            builder.drainFrom(messageQueue, config.getAsyncBatchSize() - builder.size());
                        }

                        boolean full = (builder.size() >= config.getAsyncBatchSize());
                        if ((expired || full) && builder.size() > 0) {
                            lastBatch = System.currentTimeMillis();
                            rateLimiter.pause(builder.size());
                            senders.execute(new AsyncSuroSender(builder.build(), client, config));
                            ++queuedMessageSetCount;
                        } else if (builder.size() == 0) {
                            Thread.sleep(config.getAsyncTimeout());
                        }
                    } catch (Exception e) {
                        log.error("MessageConsumer poller exception: " + e.getMessage(), e);
                    }
                }

                builder.drainFrom(messageQueue, (int) messageQueue.size());
                if (builder.size() > 0) {
                    senders.execute(new AsyncSuroSender(builder.build(), client, config));
                    ++queuedMessageSetCount;
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
            if (!poller.isTerminated()) {
                log.error("AsyncSuroClient.poller didn't terminate gracefully within {} seconds", (5 + config.getAsyncTimeout()/1000));
            }
            senders.shutdown();
            senders.awaitTermination(5000 + config.getAsyncTimeout(), TimeUnit.MILLISECONDS);
            if (!senders.isTerminated()) {
                log.error("AsyncSuroClient.senders didn't terminate gracefully within {} seconds", (5 + config.getAsyncTimeout()/1000));
            }
        } catch (InterruptedException e) {
            // ignore exceptions while shutting down
        }
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
                        new MessageSetReader(messageSet),
                        listeners));
        if (retried) {
            retriedCount.incrementAndGet();
            for (Listener listener : listeners) {
                listener.retriedCallback();
            }
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

    public static interface Listener {
        void sentCallback(int count);
        void restoredCallback();
        void lostCallback(int count);
        void retriedCallback();
    }

    private List<Listener> listeners = new CopyOnWriteArrayList<>();
    public void addListener(Listener listener) {
        listeners.add(listener);
    }
}