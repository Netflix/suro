package com.netflix.suro.nofify;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.suro.TagKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SQSNotify implements Notify {
    static Logger log = LoggerFactory.getLogger(SQSNotify.class);

    public static final String TYPE = "sqs";

    private final List<String> queues;
    private final List<String> queueUrls = new LinkedList<String>();

    @JacksonInject("sqsClient")
    private AmazonSQSClient sqsClient;
    @JacksonInject("credentials")
    private AWSCredentialsProvider credentialsProvider;

    private final ClientConfiguration clientConfig;
    private final String region;

    @Monitor(name = TagKey.SENT_COUNT, type = DataSourceType.COUNTER)
    private AtomicLong sentMessageCount = new AtomicLong(0);
    @Monitor(name = TagKey.LOST_COUNT, type = DataSourceType.COUNTER)
    private AtomicLong lostMessageCount = new AtomicLong(0);
    @Monitor(name = TagKey.RECV_COUNT, type = DataSourceType.COUNTER)
    private AtomicLong recvMessageCount = new AtomicLong(0);

    @JsonCreator
    public SQSNotify(
            @JsonProperty("queues") List<String> queues,
            @JsonProperty("region") String region,
            @JsonProperty("connectionTimeout") int connectionTimeout,
            @JsonProperty("maxConnections") int maxConnections,
            @JsonProperty("socketTimeout") int socketTimeout,
            @JsonProperty("maxRetries") int maxRetries) {
        this.queues = queues;
        this.region = region;

        Preconditions.checkArgument(queues.size() > 0);
        Preconditions.checkNotNull(region);
        Preconditions.checkArgument(connectionTimeout > 0);
        Preconditions.checkArgument(maxConnections > 0);
        Preconditions.checkArgument(socketTimeout > 0);
        Preconditions.checkArgument(maxRetries > 0);

        clientConfig = new ClientConfiguration()
                .withConnectionTimeout(connectionTimeout)
                .withMaxConnections(maxConnections)
                .withSocketTimeout(socketTimeout)
                .withMaxErrorRetry(maxRetries);
    }

    @Override
    public void init() {
        if (sqsClient == null) { // not injected
            sqsClient = new AmazonSQSClient(credentialsProvider.getCredentials(), clientConfig);
        }
        String endpoint = "sqs." + this.region + ".amazonaws.com";
        sqsClient.setEndpoint(endpoint);

        for (String queueName : queues) {
            GetQueueUrlRequest request = new GetQueueUrlRequest();
            request.setQueueName(queueName);
            queueUrls.add(sqsClient.getQueueUrl(request).getQueueUrl());
        }

        log.info(String.format("SQSNotify initialized with the endpoint: %s, queue: %s",
                endpoint, queues));
    }

    @Override
    public boolean send(String message) {
        boolean sent = false;

        try {
            for (String queueUrl : queueUrls) {
                SendMessageRequest request = new SendMessageRequest()
                        .withQueueUrl(queueUrl)
                        .withMessageBody(message);
                sqsClient.sendMessage(request);
                if (sent == false) {
                    sentMessageCount.incrementAndGet();
                    sent = true;
                }
            }
        } catch (Exception e) {
            log.error("Exception while sending SQS notification: " + e.getMessage(), e);
        }

        if (sent == false) {
            lostMessageCount.incrementAndGet();
        }

        return sent;
    }

    @Override
    public String recv() {
        ReceiveMessageRequest request = new ReceiveMessageRequest()
                .withQueueUrl(queueUrls.get(0))
                .withMaxNumberOfMessages(1);

        try {
            ReceiveMessageResult result = sqsClient.receiveMessage(request);
            if (result.getMessages().isEmpty() == false) {
                recvMessageCount.incrementAndGet();
                return result.getMessages().get(0).getBody();
            } else {
                return "";
            }
        } catch (Exception e) {
            log.error("Exception while recving SQS notification: " + e.getMessage(), e);
            return "";
        }
    }

    @Override
    public String getStat() {
        return String.format("SQSNotify with the queues: %s, sent : %d, received: %d, dropped: %d",
                queues, sentMessageCount.get(), recvMessageCount.get(), lostMessageCount.get());
    }
}
