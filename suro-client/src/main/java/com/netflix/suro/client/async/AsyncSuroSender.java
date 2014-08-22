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

import com.netflix.suro.ClientConfig;
import com.netflix.suro.connection.ConnectionPool;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageSetReader;
import com.netflix.suro.thrift.Result;
import com.netflix.suro.thrift.ResultCode;
import com.netflix.suro.thrift.TMessageSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The sender that actually sends out messages. It can be scheduled by an {@link java.util.concurrent.Executor}
 * and therefore be used to send out messages asynchronously. It also retries if a message failed to be sent.
 */
public class AsyncSuroSender implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(AsyncSuroSender.class);

    private final AsyncSuroClient client;
    private final TMessageSet messageSet;
    private final ConnectionPool connectionPool;
    private final ClientConfig config;

    public AsyncSuroSender(
            TMessageSet messageSet,
            AsyncSuroClient client,
            ClientConfig config) {
        this.messageSet = messageSet;
        this.client = client;
        this.connectionPool = client.getConnectionPool();
        this.config = config;
    }

    public void run() {
        boolean sent = false;
        boolean retried = false;
        long startTS = System.currentTimeMillis();

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
                client.updateSenderException();
            }
        }

        if (sent){
            client.updateSendTime(System.currentTimeMillis() - startTS);
            client.updateSentDataStats(messageSet, retried);
        } else {
            for (Message m : new MessageSetReader(messageSet)) {
                client.restore(m);
            }
        }
    }

    public TMessageSet getMessageSet() {
        return messageSet;
    }
}
