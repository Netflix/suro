package com.netflix.suro.sink.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.queue.MemoryQueue4Sink;
import com.netflix.suro.queue.MessageQueue4Sink;
import com.netflix.suro.sink.QueuedSink;
import com.netflix.suro.sink.Sink;
import kafka.javaapi.producer.Producer;
import kafka.metrics.KafkaMetricsReporter$;
import kafka.producer.*;
import kafka.serializer.DefaultEncoder;
import kafka.serializer.NullEncoder;
import kafka.utils.VerifiableProperties;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka 0.8 Sink
 *
 * @author jbae
 */
public class KafkaSink extends QueuedSink implements Sink {
    public final static String TYPE = "Kafka";

    private String clientId;
    private final Map<String, String> keyTopicMap;

    private final Producer<Long, byte[]> producer;
    private long msgId = 0;

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
            @JsonProperty("kafka.etc") Properties etcProps,
            @JsonProperty("keyTopicMap") Map<String, String> keyTopicMap
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

        if (etcProps != null) {
            props.putAll(etcProps);
        }

        this.keyTopicMap = keyTopicMap != null ? keyTopicMap : Maps.<String, String>newHashMap();

        producer = new Producer<Long, byte[]>(new ProducerConfig(props));
        KafkaMetricsReporter$.MODULE$.startReporters(new VerifiableProperties(props));
    }

    @Override
    public void writeTo(MessageContainer message) {
        long key = msgId++;
        if (!keyTopicMap.isEmpty()) {
            try {
                Map<String, Object> msgMap = message.getEntity(new TypeReference<Map<String, Object>>() {});
                Object keyField = msgMap.get(keyTopicMap.get(message.getRoutingKey()));
                if (keyField != null) {
                    key = keyField.hashCode();
                }
            } catch (Exception e) {
                log.error("Exception on getting key field: " + e.getMessage());
            }
        }

        enqueue(new SuroKeyedMessage(key, message.getMessage()));
    }

    @Override
    public void open() {
        setName(KafkaSink.class.getSimpleName() + "-" + clientId);
        start();
    }

    @Override
    protected void beforePolling() throws IOException { /*do nothing */}

    @Override
    protected void write(List<Message> msgList) {
        send(msgList);
    }

    @Override
    protected void innerClose() throws IOException {
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

    private List<KeyedMessage<Long, byte[]>> kafkaMsgList = new ArrayList<KeyedMessage<Long, byte[]>>();
    protected void send(List<Message> msgList) {
        for (Message m : msgList) {
            SuroKeyedMessage keyedMessage = (SuroKeyedMessage) m;
            kafkaMsgList.add(new KeyedMessage<Long, byte[]>(
                    keyedMessage.getRoutingKey(),
                    keyedMessage.getKey(),
                    keyedMessage.getPayload()));
        }

        producer.send(kafkaMsgList);
        kafkaMsgList.clear();
    }
}
