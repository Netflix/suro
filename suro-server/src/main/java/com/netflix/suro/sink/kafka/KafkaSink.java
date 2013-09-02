package com.netflix.suro.sink.kafka;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.serde.MessageSerDe;
import com.netflix.suro.message.serde.SerDe;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.queue.MemoryQueue4Sink;
import com.netflix.suro.sink.queue.MessageQueue4Sink;
import kafka.producer.ProducerStats;
import kafka.producer.ProducerTopicStats;
import kafka.serializer.DefaultEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaSink extends Thread implements Sink {
    static Logger log = LoggerFactory.getLogger(KafkaSink.class);

    public final static String TYPE = "kafka";

    private String clientId;
    private MessageQueue4Sink queue4Sink;

    protected final KafkaProducer producer;
    protected SerDe<Message> msgSerDe = new MessageSerDe();

    private final int batchSize;
    private final int batchTimeout;

    @JsonCreator
    public KafkaSink(
            @JsonProperty("queue4Sink") MessageQueue4Sink queue4Sink,
            @JsonProperty("client.id") String clientId,
            @JsonProperty("metadata.broker.list") String brokerList,
            @JsonProperty("compression.codec") String codec,
            @JsonProperty("send.buffer.bytes") int sendBufferBytes,
            @JsonProperty("batchSize") int batchSize,
            @JsonProperty("batchTimeout") int batchTimeout,
            @JsonProperty("request.timeout.ms") int requestTimeout,
            @JsonProperty("request.required.acks") Integer acks,
            @JsonProperty("message.send.max.retries") int maxRetries,
            @JsonProperty("retry.backoff.ms") int retryBackoff,
            @JsonProperty("kafka.metrics") Properties metricsProps
    ) {
        this.queue4Sink = queue4Sink == null ? new MemoryQueue4Sink(10000) : queue4Sink;

        Preconditions.checkNotNull(brokerList);
        Preconditions.checkNotNull(acks);
        Preconditions.checkNotNull(clientId);

        Properties props = new Properties();
        props.put("client.id", clientId);
        this.clientId = clientId;
        props.put("metadata.broker.list", brokerList);
        if (codec != null) {
            props.put("compression.codec", codec);
        }
        props.put("reconnect.interval", Integer.toString(Integer.MAX_VALUE));
        if (sendBufferBytes > 0) {
            props.put("send.buffer.bytes", Integer.toString(sendBufferBytes));
        }

        this.batchSize = batchSize == 0 ? 200 : batchSize;
        this.batchTimeout = batchTimeout == 0 ? 1000 : batchTimeout;

        if (requestTimeout > 0) {
            props.put("request.timeout.ms", Integer.toString(requestTimeout));
        }
        props.put("request.required.acks", acks.toString());

        if (maxRetries > 0) {
            props.put("message.send.max.retries", Integer.toString(maxRetries));
        }
        if (retryBackoff > 0) {
            props.put("retry.backoff.ms", Integer.toString(retryBackoff));
        }
        props.put("serializer.class", DefaultEncoder.class.getName());

        if (metricsProps != null) {
            props.putAll(metricsProps);
        }

        producer = new KafkaProducer(props);
    }

    @Override
    public void writeTo(Message message) {
        queue4Sink.put(message);
    }

    @Override
    public void open() {
        setName(KafkaSink.class.getSimpleName() + "-" + clientId);
        start();
    }

    private long lastBatch = System.currentTimeMillis();
    private boolean isRunning;
    private boolean isStopped;

    @Override
    public void run() {
        isRunning = true;
        List<Message> msgList = new ArrayList<Message>();

        while (isRunning) {
            try {
                Message msg = queue4Sink.poll(
                        Math.max(0, lastBatch + batchTimeout - System.currentTimeMillis()),
                        TimeUnit.MILLISECONDS);
                boolean expired = (msg == null);
                if (expired == false) {
                    msgList.add(msg);
                    queue4Sink.drain(batchSize - msgList.size(), msgList);
                }
                boolean full = (msgList.size() >= batchSize);
                if ((expired || full) && msgList.size() > 0) {
                    send(msgList);
                    afterSend(msgList);
                }
            } catch (Exception e) {
                log.error("Exception on running: " + e.getMessage(), e);
            }
        }
        log.info("Shutdown request exit loop ..., queue.size at exit time: " + queue4Sink.size());
        try {
            while (queue4Sink.isEmpty() == false) {
                if (queue4Sink.drain(batchSize, msgList) > 0) {
                    send(msgList);
                    afterSend(msgList);
                }
            }

            log.info("Shutdown request internalClose done ...");
        } catch (Exception e) {
            log.error("Exception on terminating: " + e.getMessage(), e);
        } finally {
            isStopped = true;
        }
    }

    private void afterSend(List<Message> msgList) {
        msgList.clear();
        queue4Sink.commit();
        lastBatch = System.currentTimeMillis();
    }

    @Override
    public void close() {
        isRunning = false;
        log.info("Starting to close");
        do {
            try {
                Thread.sleep(500);
            } catch (Exception ignored) {
                log.error("ignore an exception on close");
            }
        } while (isStopped == false);

        queue4Sink.close();
        producer.close();
    }

    @Override
    public String recvNotify() {
        return null;
    }

    @Override
    public String getStat() {
        ProducerStats stats = kafka.producer.ProducerStatsRegistry.getProducerStats(clientId);
        ProducerTopicStats topicStats = kafka.producer.ProducerTopicStatsRegistry.getProducerTopicStats(clientId);

        StringBuilder sb = new StringBuilder();
        sb.append("resend rate: ").append(stats.resendRate().count()).append('\n');
        sb.append("serialization error rate: " ).append(stats.serializationErrorRate().count()).append('\n');
        sb.append("failed send rate: ").append(stats.failedSendRate().count()).append("\n\n");
        sb.append("message rate: ").append(topicStats.getProducerAllTopicsStats().messageRate().count()).append('\n');
        sb.append("byte rate: " ).append(topicStats.getProducerAllTopicsStats().byteRate().count()).append('\n');
        sb.append("dropped message rate: " ).append(topicStats.getProducerAllTopicsStats().droppedMessageRate().count()).append('\n');

        return sb.toString();
    }

    protected void send(List<Message> msgList) {
        try {
            producer.send(msgList, msgSerDe);
        } catch (Exception e) {
            log.error("Exception on send: " + e.getMessage(), e);
        }
    }
}

