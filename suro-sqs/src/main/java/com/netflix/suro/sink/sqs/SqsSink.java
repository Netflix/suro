package com.netflix.suro.sink.sqs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.schlep.producer.MessageProducer;
import com.netflix.schlep.producer.OutgoingMessage;
import com.netflix.schlep.sqs.producer.SqsMessageProducer;
import com.netflix.suro.message.Message;
import com.netflix.suro.sink.Sink;

/**
 * SQS sink implementation using Schlep
 * @author elandau
 *
 */
public class SqsSink implements Sink {
    static Logger log = LoggerFactory.getLogger(SqsSink.class);

    public static final String TYPE = "sqs";
    
    public final MessageProducer producer;
    
    @JsonCreator
    public SqsSink(
            @JacksonInject("credentials")       AWSCredentialsProvider credentialProvider,
            @JsonProperty("region")             String region,
            @JsonProperty("maxBacklog")         int maxBacklog,
            @JsonProperty("threadCount")        int threadCount,
            @JsonProperty("connectTimeout")     int connectTimeout,
            @JsonProperty("readTimeout")        int readTimeout,
            @JsonProperty("retryCount")         int retries,
            @JsonProperty("batchSize")          int batchSize,
            @JsonProperty("batchTimeout")       int batchTimeout) throws Exception {
        
        producer = SqsMessageProducer.builder()
                .withBatchSize(batchSize)
                .withBufferDelay(batchTimeout)
                .withCredentials(credentialProvider.getCredentials())
                .withMaxBacklog(maxBacklog)
                .withConnectionTimeout(connectTimeout)
                .withMaxRetries(retries)
                .withReadTimeout(readTimeout)
                .withRegion(region)
                .withThreadCount(threadCount)
                .build();
    }
    
    @Override
    public void writeTo(Message message) {
        producer.send(OutgoingMessage.builder().withMessage(message).build());
    }

    @Override
    public void open() {
        try {
            producer.start();
        } catch (Exception e) {
            log.error("Failed to start sqs producer");
        }
    }

    @Override
    public void close() {
        try {
            producer.stop();
        } catch (Exception e) {
            log.error("Failed to stop sqs producer");
        }
    }

    @Override
    public String recvNotify() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getStat() {
        // TODO Auto-generated method stub
        return null;
    }

}
