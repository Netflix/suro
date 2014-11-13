package com.netflix.suro.sink;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.suro.client.SuroClient;
import com.netflix.suro.message.MessageContainer;

import java.util.Properties;

/**
 * Sink implementation of suro client for sending messages to suro server
 *
 * @author jbae
 */
public class SuroSink implements Sink {
    public static final String TYPE = "suro";

    private SuroClient client;
    private final Properties props;

    @JsonCreator
    public SuroSink(@JsonProperty("properties") Properties props) {
        this.props = props;
    }

    @Override
    public void writeTo(MessageContainer message) {
        client.send(message.getMessage());
    }

    @Override
    public void open() {
        client = new SuroClient(props);
    }

    @Override
    public void close() {
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
    public long checkPause() {
        return 0;
    }
}
