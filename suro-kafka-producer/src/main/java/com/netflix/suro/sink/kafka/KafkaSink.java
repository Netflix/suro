package com.netflix.suro.sink.kafka;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.suro.TagKey;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.message.StringMessage;
import com.netflix.suro.sink.Sink;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action3;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka 0.8 Sink
 *
 * @author jbae
 */
public class KafkaSink implements Sink {
    private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);

    public final static String TYPE = "Kafka";

    private final Map<String, String> keyTopicMap;
    private final boolean blockOnBufferFull;
    private final Properties props;

    private KafkaProducer<byte[], byte[]> producer;
    private final KafkaRetentionPartitioner retentionPartitioner;
    private final Set<String> metadataFetchedTopicSet;
    private final BlockingQueue<MessageContainer> metadataWaitingQueue;
    private final ExecutorService executor;

    private final static MessageContainer SHUTDOWN_POISON_MSG = new StringMessage("suro-KafkaSink-shutdownMsg-routingKey",
        "suro-KafkaSink-shutdownMsg-body");

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
            @JsonProperty("metadata.waiting.queue.size") int metadataWaitingQueueSize,
            @JsonProperty("kafka.etc") Properties etcProps,
            @JsonProperty("keyTopicMap") Map<String, String> keyTopicMap,
            @JacksonInject KafkaRetentionPartitioner retentionPartitioner) {
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

        this.metadataFetchedTopicSet = new CopyOnWriteArraySet<>();
        this.metadataWaitingQueue = new ArrayBlockingQueue<>(metadataWaitingQueueSize > 0 ? metadataWaitingQueueSize : 1024);
        this.executor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(false).setNameFormat("KafkaSink-MetadataFetcher-%d").build());
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

    private AtomicLong queuedRecords = new AtomicLong(0);
    private AtomicLong sentRecords = new AtomicLong(0);
    @VisibleForTesting
    protected AtomicLong droppedRecords = new AtomicLong(0);
    private volatile Action3 recordCounterListener;
    public void setRecordCounterListener(Action3 action) {
        this.recordCounterListener = action;
    }

    @Override
    public void writeTo(final MessageContainer message) {
        queuedRecords.incrementAndGet();
        DynamicCounter.increment(
            MonitorConfig
                .builder("queuedRecord")
                .withTag(TagKey.ROUTING_KEY, message.getRoutingKey())
                .build());
        runRecordCounterListener();

        if (metadataFetchedTopicSet.contains(message.getRoutingKey())) {
            sendMessage(message);
        } else {
            if(!metadataWaitingQueue.offer(message)) {
                dropMessage(message.getRoutingKey(), "metadataWaitingQueueFull");
            }
        }
    }

    private void runRecordCounterListener() {
        if (recordCounterListener != null) {
            recordCounterListener.call(queuedRecords.get(), sentRecords.get(), droppedRecords.get());
        }
    }

    private void sendMessage(final MessageContainer message) {
        try {
            List<PartitionInfo> partitionInfos = producer.partitionsFor(message.getRoutingKey());
            int partition = retentionPartitioner.getKey(message.getRoutingKey(), partitionInfos);

            if (!keyTopicMap.isEmpty()) {
                try {
                    Map<String, Object> msgMap = message.getEntity(new TypeReference<Map<String, Object>>() {
                    });
                    Object keyField = msgMap.get(keyTopicMap.get(message.getRoutingKey()));
                    if (keyField != null) {
                        long hashCode = keyField.hashCode();
                        partition = Math.abs((int) (hashCode ^ (hashCode >>> 32))) % partitionInfos.size();
                    }
                } catch (Exception e) {
                    log.error("Exception on getting key field: " + e.getMessage());
                }
            }

            producer.send(
                new ProducerRecord(message.getRoutingKey(), partition, null, message.getMessage().getPayload()),
                new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            log.error("Exception while sending", e);
                            DynamicCounter.increment(
                                MonitorConfig
                                    .builder("failedRecord")
                                    .withTag(TagKey.ROUTING_KEY, message.getRoutingKey())
                                    .build());
                            droppedRecords.incrementAndGet();
                            runRecordCounterListener();
                        } else {
                            DynamicCounter.increment(
                                MonitorConfig
                                    .builder("sentRecord")
                                    .withTag(TagKey.ROUTING_KEY, message.getRoutingKey())
                                    .build());
                            sentRecords.incrementAndGet();
                            runRecordCounterListener();
                        }
                    }
                });
        }
         catch (Throwable e) {
            log.error("Exception before sending", e);
            dropMessage(message.getRoutingKey(), e.getClass().getName());
        }
    }

    @Override
    public void open() {
        producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());

        executor.submit(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    final MessageContainer message;
                    try {
                        message = metadataWaitingQueue.poll(1, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        continue;
                    }
                    if(message == null) {
                        continue;
                    }
                    // check poison msg for shutdown
                    if(message == SHUTDOWN_POISON_MSG) {
                        break;
                    }
                    try {
                        if (!metadataFetchedTopicSet.contains(message.getRoutingKey())) {
                            producer.partitionsFor(message.getRoutingKey());
                            metadataFetchedTopicSet.add(message.getRoutingKey());
                        }
                        sendMessage(message);
                    } catch(Throwable t) {
                        log.error("failed to get metadata: " + message.getRoutingKey(), t);
                        // try to put back to the queue if there is still space
                        if(!metadataWaitingQueue.offer(message)) {
                            dropMessage(message.getRoutingKey(), "metadataWaitingQueueFull");
                        }
                    }
                }
            }
        });

    }

    @Override
    public void close() {
        try {
            // try to insert a poison msg for shutdown
            // ignore success or failure
            metadataWaitingQueue.offer(SHUTDOWN_POISON_MSG);
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
        try {
            producer.close();
        } catch(Exception e) {
            log.error("failed to close kafka producer", e);
        }
    }

    @Override
    public String recvNotice() {
        return null;
    }

    @Override
    public String getStat() {
        Map<MetricName,? extends Metric> metrics = producer.metrics();
        StringBuilder sb = new StringBuilder();
        // add kafka producer stats, which are rates
        for( Map.Entry<MetricName,? extends Metric> e : metrics.entrySet() ){
            sb.append("kafka.").append(e.getKey()).append(": ").append(e.getValue().value()).append('\n');
        }

        return sb.toString();
    }

    @Override
    public long getNumOfPendingMessages() {
        return queuedRecords.get() - sentRecords.get() - droppedRecords.get();
    }

    @Override
    public long checkPause() {
        if (blockOnBufferFull) {
            return 0; // do not pause here, will be blocked
        } else {
            //producer.metrics().get(new MetricName("buffer-total-bytes", "producer-metrics", "desc", "client-id", "kafkasink"))
            double totalBytes = producer.metrics().get(
                new MetricName(
                    "buffer-total-bytes",
                    "producer-metrics",
                    "desc",
                    "client-id",
                    props.getProperty("client.id"))).value();
            double availableBytes = producer.metrics().get(
                new MetricName(
                    "buffer-available-bytes",
                    "producer-metrics",
                    "desc",
                    "client-id",
                    props.getProperty("client.id"))).value();

            double consumedMemory = totalBytes - availableBytes;
            double memoryRate = consumedMemory / totalBytes;
            if (memoryRate >= 0.5) {
                double outgoingRate = producer.metrics().get(
                    new MetricName(
                        "outgoing-byte-rate",
                        "producer-metrics",
                        "desc",
                        "client-id",
                        props.getProperty("client.id"))).value();
                double throughputRate = Math.max(outgoingRate, 1.0);
                return (long) (consumedMemory / throughputRate * 1000);
            } else {
                return 0;
            }
        }
    }

    private void dropMessage(final String routingKey, final String reason) {
        DynamicCounter.increment(
            MonitorConfig
                .builder("droppedRecord")
                .withTag(TagKey.ROUTING_KEY, routingKey)
                .withTag(TagKey.DROPPED_REASON, reason)
                .build());
        droppedRecords.incrementAndGet();
        runRecordCounterListener();
    }
}
