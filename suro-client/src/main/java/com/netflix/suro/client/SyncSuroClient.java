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

package com.netflix.suro.client;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.TagKey;
import com.netflix.suro.client.async.AsyncSuroClient;
import com.netflix.suro.connection.ConnectionPool;
import com.netflix.suro.message.Compression;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageSetBuilder;
import com.netflix.suro.message.MessageSetReader;
import com.netflix.suro.thrift.Result;
import com.netflix.suro.thrift.ResultCode;
import com.netflix.suro.thrift.TMessageSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Synchronous {@link ISuroClient} implementation
 * Sends a message or TMessageSet one by one
 * @author jbae
 */
public class SyncSuroClient implements ISuroClient {
    private static final Logger log = LoggerFactory.getLogger(SyncSuroClient.class);

    private final ClientConfig config;
    private final ConnectionPool connectionPool;
    private final Compression compression;

    @Inject
    public SyncSuroClient(ClientConfig config, ConnectionPool connectionPool) {
        this.config = config;
        this.connectionPool = connectionPool;
        this.compression = Compression.create(config.getCompression());

        Monitors.registerObject(this);
    }

    private AtomicLong sentMessageCount = new AtomicLong(0);
    @Override
    public long getSentMessageCount() {
        return sentMessageCount.get();
    }

    private AtomicLong lostMessageCount = new AtomicLong(0);
    @Override
    public long getLostMessageCount() {
        return lostMessageCount.get();
    }

    @Override
    public long getNumOfPendingMessages() {
        return 0;
    }

    private AtomicLong retriedCount = new AtomicLong(0);
    public long getRetriedCount() {
        return retriedCount.get();
    }

    @Monitor(name = "senderExceptionCount", type = DataSourceType.COUNTER)
    private AtomicLong senderExceptionCount = new AtomicLong(0);

    @Override
    public void send(Message message) {
        send(new MessageSetBuilder(config)
                .withCompression(compression)
                .withMessage(message.getRoutingKey(), message.getPayload()).build());
    }

    private List<AsyncSuroClient.Listener> emptyList = Lists.newArrayList();

    public boolean send(TMessageSet messageSet) {
        if (messageSet == null) {
            return false;
        }

        boolean sent = false;
        boolean retried = false;

        for (int i = 0; i < config.getRetryCount(); ++i) {
            ConnectionPool.SuroConnection connection = connectionPool.chooseConnection();
            if (connection == null) {
                continue;
            }
            try {
                Result result = connection.send(messageSet);
                if (result != null && result.getResultCode() == ResultCode.OK && result.isSetMessage()) {
                    sent = true;
                    connectionPool.endConnection(connection);
                    retried = i > 0;
                    break;
                } else {
                    log.error("Server is not stable: " + connection.getServer().toString());
                    connectionPool.markServerDown(connection);
                    try { Thread.sleep(Math.min(i + 1, 5) * 100); } catch (InterruptedException e) {} // ignore an exception
                }
            } catch (Exception e) {
                log.error("Exception in send: " + e.getMessage(), e);
                connectionPool.markServerDown(connection);
            }
        }

        MessageSetReader reader = new MessageSetReader(messageSet);
        if (sent) {
            sentMessageCount.addAndGet(incrementMessageCount(TagKey.SENT_COUNT, config.getApp(), reader, emptyList));
            if (retried) {
                retriedCount.incrementAndGet();
            }

        } else {
            lostMessageCount.addAndGet(incrementMessageCount(TagKey.LOST_COUNT, config.getApp(), reader, emptyList));
        }

        return sent;
    }

    public static int incrementMessageCount(String counterName, String app, Iterable<Message> messages, List<AsyncSuroClient.Listener> listeners) {
        int count = 0;
        for (Message message : messages) {
            DynamicCounter.increment(
                    MonitorConfig.builder(counterName)
                            .withTag(TagKey.APP, app)
                            .withTag(TagKey.DATA_SOURCE, message.getRoutingKey())
                            .build());
            ++count;
        }

        for (AsyncSuroClient.Listener listener : listeners) {
            listener.sentCallback(count);
        }

        return count;
    }
}
