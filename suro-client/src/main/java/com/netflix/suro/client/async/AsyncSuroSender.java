package com.netflix.suro.client.async;

import com.netflix.suro.ClientConfig;
import com.netflix.suro.connection.ConnectionPool;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageSetReader;
import com.netflix.suro.thrift.ResultCode;
import com.netflix.suro.thrift.TMessageSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        if (sent == true){
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
