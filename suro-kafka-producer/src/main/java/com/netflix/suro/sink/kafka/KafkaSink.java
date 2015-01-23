package com.netflix.suro.sink.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.suro.TagKey;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.sink.Sink;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Action3;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
    private final MetadataWaitingQueuePolicy metadataWaitingQueuePolicy;

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
            @JsonProperty("keyTopicMap") Map<String, String> keyTopicMap
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

        this.metadataWaitingQueuePolicy = new MetadataWaitingQueuePolicy(
            metadataWaitingQueueSize == 0 ? 10000 : metadataWaitingQueueSize,
            blockOnBufferFull);
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
    private AtomicLong droppedRecords = new AtomicLong(0);
    private volatile Action3 recordCounterListener;
    public void setRecordCounterListener(Action3 action) {
        this.recordCounterListener = action;
    }

    private Set<String> metadataFetchedTopicSet = new CopyOnWriteArraySet<String>();
    private PublishSubject<MessageContainer> stream = PublishSubject.create();
    private Subscription subscription;
    private ExecutorService executor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setDaemon(false).setNameFormat("kafka-metadata-fetcher-%d").build());

    @Override
    public void writeTo(final MessageContainer message) {
        queuedRecords.incrementAndGet();
        runRecordCounterListener();

        if (metadataFetchedTopicSet.contains(message.getRoutingKey())) {
            sendMessage(message);
        } else {
            stream.onNext(message);
        }
    }

    private void runRecordCounterListener() {
        if (recordCounterListener != null) {
            recordCounterListener.call(queuedRecords.get(), sentRecords.get(), droppedRecords.get());
        }
    }

    private void sendMessage(final MessageContainer message) {
        byte[] key = null;
        if (!keyTopicMap.isEmpty() && keyTopicMap.get(message.getRoutingKey()) != null) {
            try {
                Map<String, Object> msgMap = message.getEntity(new TypeReference<Map<String, Object>>() {
                });
                Object keyField = msgMap.get(keyTopicMap.get(message.getRoutingKey()));
                if (keyField != null) {
                    key = keyField.toString().getBytes();
                }
            } catch (Exception e) {
                log.error("Exception on getting key field: " + e.getMessage());
            }
        }

        try {
            producer.send(
                new ProducerRecord(message.getRoutingKey(), null, key, message.getMessage().getPayload()),
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
        } catch (Exception e) {
            log.error("Exception before sending", e);
            dropMessage(message);
        }
    }

    private void dropMessage(MessageContainer message) {
        DynamicCounter.increment(
            MonitorConfig
                .builder("droppedRecord")
                .withTag(TagKey.ROUTING_KEY, message.getRoutingKey())
                .build());
        droppedRecords.incrementAndGet();
        runRecordCounterListener();
    }

    @Override
    public void open() {
        producer = new KafkaProducer(props);
        subscription = stream
                .filter(new Func1<MessageContainer, Boolean>() {
                    @Override
                    public Boolean call(MessageContainer message) {
                        if (!metadataWaitingQueuePolicy.acquire()) {
                            dropMessage(message);
                            return false;
                        } else {
                            return true;
                        }
                    }
                })
                .observeOn(Schedulers.from(executor))
                .subscribe(
                        new Action1<MessageContainer>() {
                            @Override
                            public void call(final MessageContainer message) {
                                try {
                                    if (!metadataFetchedTopicSet.contains(message.getRoutingKey())) {
                                        producer.partitionsFor(message.getRoutingKey());
                                        metadataFetchedTopicSet.add(message.getRoutingKey());
                                    }
                                    sendMessage(message);
                                } catch (Exception e) {
                                    log.error("Exception on waiting for metadata", e);
                                    stream.onNext(message); // try again
                                } finally {
                                    metadataWaitingQueuePolicy.release();
                                }
                            }
                        },
                        new Action1<Throwable>() {
                            @Override
                            public void call(Throwable throwable) {
                                log.error("Exception on stream", throwable);
                            }
                        }
                );
    }

    @Override
    public void close() {
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
        subscription.unsubscribe();
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
        return queuedRecords.get() - sentRecords.get() - droppedRecords.get();
    }

    private static class MetadataWaitingQueuePolicy {
        private final Semaphore semaphore;
        private final boolean blockingOnBufferFull;

        public MetadataWaitingQueuePolicy(int capacity, boolean blockOnBufferFull) {
            semaphore = new Semaphore(capacity);
            this.blockingOnBufferFull = blockOnBufferFull;
        }

        public boolean acquire() {
            if (blockingOnBufferFull) {
                try {
                    semaphore.acquire();
                    return true;
                } catch (InterruptedException e) {
                    // ignore
                    return true;
                }
            } else {
                return semaphore.tryAcquire();
            }
        }
        public void release() {
            semaphore.release();
        }

    }
}
