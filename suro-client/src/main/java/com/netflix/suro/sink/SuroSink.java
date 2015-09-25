package com.netflix.suro.sink;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.suro.TagKey;
import com.netflix.suro.client.SuroClient;
import com.netflix.suro.message.MessageContainer;
import org.apache.commons.lang.StringUtils;

import java.util.Properties;

/**
 * Sink implementation of suro client for sending messages to suro server
 *
 * @author jbae
 */
public class SuroSink implements Sink {
    public static final String TYPE = "suro";

    private volatile boolean isOpened = false;
    private volatile SuroClient client;
    private final Properties props;

    @JsonCreator
    public SuroSink(@JsonProperty("properties") Properties props) {
        this.props = props;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void writeTo(MessageContainer message) {
        if(!isOpened) {
            dropMessage(getRoutingKey(message), "sinkNotOpened");
            return;
        }
        client.send(message.getMessage());
    }

    private void dropMessage(final String routingKey, final String reason) {
        DynamicCounter.increment(
                MonitorConfig
                        .builder("droppedMessageCount")
                        .withTag(TagKey.ROUTING_KEY, routingKey)
                        .withTag(TagKey.DROPPED_REASON, reason)
                        .build());
    }

    private String getRoutingKey(final MessageContainer message) {
        String routingKey = message.getRoutingKey();
        if(StringUtils.isBlank(routingKey)) {
            routingKey = "none";
        }
        return routingKey;
    }

    @Override
    public void open() {
        client = new SuroClient(props);
        isOpened = true;
    }

    @Override
    public void close() {
        isOpened = false;
        client.shutdown();
    }

    @Override
    public String recvNotice() {
        return "";
    }

    @Override
    public String getStat() {
        return "sent: " + client.getSentMessageCount() + '\n' + "lost: " + client.getLostMessageCount();
    }

    @Override
    public long getNumOfPendingMessages() {
        return client.getNumOfPendingMessages();
    }

    @Override
    public boolean isOpened() {
        return isOpened;
    }
}
