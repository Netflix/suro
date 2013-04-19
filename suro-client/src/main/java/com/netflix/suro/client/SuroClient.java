package com.netflix.suro.client;

import com.netflix.suro.message.Message;

public interface SuroClient {
    void send(Message message);

    long getSentMessageCount();

    long getLostMessageCount();
}
