package com.netflix.suro.client;

import com.google.inject.Inject;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.TagKey;
import com.netflix.suro.connection.ConnectionPool;
import com.netflix.suro.message.*;
import com.netflix.suro.thrift.ResultCode;
import com.netflix.suro.thrift.TMessageSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class SyncSuroClient implements SuroClient {
    static Logger log = LoggerFactory.getLogger(SyncSuroClient.class);

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

    @Monitor(name = TagKey.SENT_COUNT, type = DataSourceType.COUNTER)
    private AtomicLong sentMessageCount = new AtomicLong(0);
    @Override
    public long getSentMessageCount() {
        return sentMessageCount.get();
    }
    @Monitor(name = TagKey.LOST_COUNT, type = DataSourceType.COUNTER)
    private AtomicLong lostMessageCount = new AtomicLong(0);
    @Override
    public long getLostMessageCount() {
        return lostMessageCount.get();
    }
    @Monitor(name = TagKey.RETRIED_COUNT, type = DataSourceType.COUNTER)
    private AtomicLong retriedCount = new AtomicLong(0);
    public long getRetriedCount() {
        return retriedCount.get();
    }

    @Override
    public void send(Message message) {
        send(new MessageSetBuilder()
                .withApp(config.getApp())
                .withDatatype(config.getDataType())
                .withSerDe(config.getSerDe())
                .withCompression(compression)
                .withMessage(message.getRoutingKey(), message.getPayload()).build());
    }

    public boolean send(TMessageSet messageSet) {
        boolean sent = false;
        boolean retried = false;

        for (int i = 0; i < config.getRetryCount(); ++i) {
            ConnectionPool.SuroConnection connection = connectionPool.chooseConnection();
            try {
                if (connection.send(messageSet).getResultCode() == ResultCode.OK) {
                    sent = true;
                    retried = i > 0;
                    break;
                }
            } catch (Exception e) {
                log.error("Exception in send: " + e.getMessage(), e);
                connection.disconnect();
                try {
                    connection.connect();
                } catch (Exception ex) {
                    log.error("Error in connecting to " + connection + " message: " + e.getMessage(), ex);
                    connectionPool.markServerDown(connection);
                }
            } finally {
                connectionPool.endConnection(connection);
            }
        }

        MessageSetReader reader = new MessageSetReader(messageSet);
        if (sent) {
            sentMessageCount.addAndGet(incrementMessageCount(TagKey.SENT_COUNT, reader));
            if (retried) {
                retriedCount.incrementAndGet();
            }

        } else {
            lostMessageCount.addAndGet(incrementMessageCount(TagKey.LOST_COUNT, reader));
        }

        return sent;
    }

    public static int incrementMessageCount(String counterName, Iterable<Message> messages) {
        int count = 0;
        for (Message message : messages) {
            DynamicCounter.increment(
                    MonitorConfig.builder(counterName)
                            .withTag(TagKey.APP, message.getApp())
                            .withTag(TagKey.DATA_SOURCE, message.getRoutingKey())
                            .build());
            ++count;
        }

        return count;
    }
}
