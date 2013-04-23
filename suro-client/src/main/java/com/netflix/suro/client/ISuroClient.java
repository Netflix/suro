package com.netflix.suro.client;

import com.netflix.suro.message.Message;

public interface ISuroClient {
    void send(Message message);

    long getSentMessageCount();

    long getLostMessageCount();
}
