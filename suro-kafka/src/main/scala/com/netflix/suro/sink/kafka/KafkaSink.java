package com.netflix.suro.sink.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.sink.QueuedSink;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.queue.MemoryQueue4Sink;
import com.netflix.suro.queue.MessageQueue4Sink;
import kafka.javaapi.producer.Producer;
import kafka.producer.*;
import kafka.serializer.DefaultEncoder;
import kafka.serializer.NullEncoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Kafka 0.8 Sink
 *
 * @author jbae
 */
public class KafkaSink extends QueuedSink implements Sink {
    public final static String TYPE = "Kafka";

    private String clientId;

    protected final Producer producer;

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
        Preconditions.checkNotNull(brokerList);
        Preconditions.checkNotNull(acks);
        Preconditions.checkNotNull(clientId);

        this.clientId = clientId;
        initialize(queue4Sink == null ? new MemoryQueue4Sink(10000) : queue4Sink, batchSize, batchTimeout);

        Properties props = new Properties();
        props.put("client.id", clientId);
        props.put("metadata.broker.list", brokerList);
        if (codec != null) {
            props.put("compression.codec", codec);
        }
        props.put("reconnect.interval", Integer.toString(Integer.MAX_VALUE));
        if (sendBufferBytes > 0) {
            props.put("send.buffer.bytes", Integer.toString(sendBufferBytes));
        }
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
        props.put("key.serializer.class", NullEncoder.class.getName());

        if (metricsProps != null) {
            props.putAll(metricsProps);
        }

        producer = new Producer(new ProducerConfig(props));
    }

    @Override
    public void writeTo(MessageContainer message) {
        queue4Sink.offer(message.getMessage());
    }

    @Override
    public void open() {
        setName(KafkaSink.class.getSimpleName() + "-" + clientId);
        start();
    }

    @Override
    protected void beforePolling() throws IOException { /*do nothing */}

    @Override
    protected void write(List<Message> msgList) throws IOException {
        send(msgList);
        msgList.clear();
        queue4Sink.commit();
    }

    @Override
    protected void innerClose() throws IOException {
        queue4Sink.close();
        producer.close();
    }

    @Override
    public String recvNotice() {
        return null;
    }

    @Override
    public String getStat() {
        ProducerStats stats = ProducerStatsRegistry.getProducerStats(clientId);
        ProducerTopicStats topicStats = ProducerTopicStatsRegistry.getProducerTopicStats(clientId);

        StringBuilder sb = new StringBuilder();
        sb.append("resend rate: ").append(stats.resendRate().count()).append('\n');
        sb.append("serialization error rate: " ).append(stats.serializationErrorRate().count()).append('\n');
        sb.append("failed send rate: ").append(stats.failedSendRate().count()).append('\n');
        sb.append("message rate: ").append(topicStats.getProducerAllTopicsStats().messageRate().count()).append('\n');
        sb.append("byte rate: " ).append(topicStats.getProducerAllTopicsStats().byteRate().count()).append('\n');
        sb.append("dropped message rate: " ).append(topicStats.getProducerAllTopicsStats().droppedMessageRate().count()).append('\n');

        return sb.toString();
    }

    protected long msgId = 0;
    private List<KeyedMessage<Long, byte[]>> kafkaMsgList = new ArrayList<KeyedMessage<Long, byte[]>>();
    protected void send(List<Message> msgList) {
        for (Message m : msgList) {
            kafkaMsgList.add(new KeyedMessage<Long, byte[]>(m.getRoutingKey(), msgId++, m.getPayload()));
        }
        producer.send(kafkaMsgList);
        kafkaMsgList.clear();
    }
}
