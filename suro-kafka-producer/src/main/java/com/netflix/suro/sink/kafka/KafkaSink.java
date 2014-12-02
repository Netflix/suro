package com.netflix.suro.sink.kafka;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.sink.Sink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka 0.8 Sink
 *
 * @author jbae
 */
public class KafkaSink implements Sink {
    private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);

    public final static String TYPE = "kafka";

    private final Map<String, String> keyTopicMap;
    private final boolean blockOnBufferFull;
    private final Properties props;

    private KafkaProducer producer;
    private final KafkaRetentionPartitioner retentionPartitioner;

    @JsonCreator
    public KafkaSink(
            @JsonProperty("client.id") String clientId,
            @JsonProperty("metadata.broker.list") String brokerList,
            @JsonProperty("bootstrap.servers") String bootstrapServers,
            @JsonProperty("request.required.acks") Integer requiredAcks,
            @JsonProperty("acks") String acks,
            @JsonProperty("buffer.memory") long bufferMemory,
            @JsonProperty("batch.size") int batchSize,
            @JsonProperty("compression.codec") String codec,
            @JsonProperty("compression.type") String compression,
            @JsonProperty("retries") int retries,
            @JsonProperty("block.on.buffer.full") boolean blockOnBufferFull,
            @JsonProperty("kafka.etc") Properties etcProps,
            @JsonProperty("keyTopicMap") Map<String, String> keyTopicMap,
            @JacksonInject KafkaRetentionPartitioner retentionPartitioner
    ) {
        Preconditions.checkArgument(bootstrapServers != null | brokerList != null);
        Preconditions.checkNotNull(clientId);

        props = new Properties();
        props.put("client.id", clientId);
        props.put("bootstrap.servers", brokerList != null ? brokerList : bootstrapServers);

        if (acks != null || requiredAcks != null) {
            props.put("acks", requiredAcks != null ? requiredAcks.toString() : acks);
        }
        if (bufferMemory > 0) {
            props.put("buffer.memory", bufferMemory);
        }
        if (batchSize > 0) {
            props.put("batch.size", batchSize);
        }
        if (compression != null || codec != null) {
            props.put("compression.type", codec != null ? codec : compression);
        }
        if (retries > 0) {
            props.put("retries", retries);
        }

        this.blockOnBufferFull = blockOnBufferFull;
        props.put("block.on.buffer.full", blockOnBufferFull);
        setServoReporter();

        if (etcProps != null) {
            props.putAll(etcProps);
        }

        this.keyTopicMap = keyTopicMap != null ? keyTopicMap : Maps.<String, String>newHashMap();

        this.retentionPartitioner = retentionPartitioner;
    }

    private void setServoReporter() {
        props.put("metric.reporters", Lists.newArrayList(ServoReporter.class.getName()));
        // this should be needed because ProducerConfig cannot retrieve undefined key
        try {
            Field f = ProducerConfig.class.getDeclaredField("config");
            f.setAccessible(true);
            ConfigDef config = (ConfigDef) f.get(ConfigDef.class);
            config.define(ServoReporter.class.getName(), ConfigDef.Type.CLASS, ServoReporter.class, ConfigDef.Importance.LOW, "");
        } catch (Exception e) {
            // swallow exception
        }
        props.put(ServoReporter.class.getName(), ServoReporter.class);
    }

    @Override
    public void writeTo(MessageContainer message) {
        String routingKey = message.getRoutingKey().toLowerCase();

        int numPartitions = producer.partitionsFor(routingKey).size();
        int partition = (int) Math.abs(retentionPartitioner.getKey() % numPartitions);

        if (!keyTopicMap.isEmpty()) {
            try {
                Map<String, Object> msgMap = message.getEntity(new TypeReference<Map<String, Object>>() {});
                Object keyField = msgMap.get(keyTopicMap.get(routingKey));
                if (keyField != null) {
                    long hashCode = keyField.hashCode();
                    partition = Math.abs((int)(hashCode ^ (hashCode >>> 32))) % numPartitions;
                }
            } catch (Exception e) {
                log.error("Exception on getting key field: " + e.getMessage());
            }
        }

        producer.send(new ProducerRecord(routingKey, partition, null, message.getMessage().getPayload()));
    }

    @Override
    public void open() {
        producer = new KafkaProducer(props);
    }

    @Override
    public void close() {
        producer.close();
    }

    @Override
    public String recvNotice() {
        return null;
    }

    @Override
    public String getStat() {
        Map<String,? extends Metric> metrics = producer.metrics();
        StringBuilder sb = new StringBuilder();
        // add kafka producer stats, which are rates
        for( Map.Entry<String,? extends Metric> e : metrics.entrySet() ){
            sb.append("kafka.").append(e.getKey()).append(": ").append(e.getValue().value()).append('\n');
        }

        return sb.toString();
    }

    @Override
    public long getNumOfPendingMessages() {
        // we cannot get the exact number of pending messages here but
        // this should return non-zero if it's still sending messages
        // for graceful termination
        if (producer.metrics().get("buffer-total-bytes").value() !=
            producer.metrics().get("buffer-available-bytes").value()) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public long checkPause() {
        if (blockOnBufferFull) {
            return 0; // do not pause here, will be blocked
        } else {
            double consumedMemory =
                    producer.metrics().get("buffer-total-bytes").value()
                            - producer.metrics().get("buffer-available-bytes").value();
            double memoryRate = consumedMemory / producer.metrics().get("buffer-total-bytes").value();
            if (memoryRate >= 0.5) {
                double throughputRate = Math.max(producer.metrics().get("outgoing-byte-rate").value(), 1.0);
                return (long) (consumedMemory / throughputRate * 1000);
            } else {
                return 0;
            }
        }
    }
}
