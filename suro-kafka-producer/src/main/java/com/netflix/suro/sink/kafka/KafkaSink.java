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
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.suro.TagKey;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.sink.Sink;
import org.apache.commons.lang.StringUtils;
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
import java.util.ArrayList;
import java.util.List;
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

    private final boolean normalizeRoutingKey;
    private final String bootstrapServers;
    private final String vipAddress;
    private final Partitioner partitioner;
    private final MetadataWaitingQueuePolicy metadataWaitingQueuePolicy;
    private final Properties props;
    private final Map<String, String> keyTopicMap;

    private KafkaProducer producer;

    @JsonCreator
    public KafkaSink(
            @JsonProperty("normalizeRoutingKey") boolean normalizeRoutingKey,
            @JsonProperty("client.id") String clientId,
            @JsonProperty("bootstrap.servers") String bootstrapServers,
            @JsonProperty("vipAddress") String vipAddress,
            @JsonProperty("partitioner") String partitionerSelector,
            @JsonProperty("partitioner.sticky.interval") int stickyInterval,
            @JsonProperty("block.on.metadata.queue.full") boolean blockOnMetadataQueueFull,
            @JsonProperty("metadata.waiting.queue.size") int metadataWaitingQueueSize,
            @JsonProperty("keyTopicMap") Map<String, String> keyTopicMap,
            @JsonProperty("kafka.etc") Properties etcProps,
            @JacksonInject DiscoveryClient discoveryClient
    ) {
        Preconditions.checkArgument(bootstrapServers != null);
        Preconditions.checkArgument(clientId != null);

        this.normalizeRoutingKey = normalizeRoutingKey;
        this.bootstrapServers = bootstrapServers;
        this.vipAddress = vipAddress;

        if(StickyPartitioner.NAME.equals(partitionerSelector)) {
            int intervalMs = stickyInterval > 0 ? stickyInterval : 1000;
            partitioner = StickyPartitioner.builder()
                .withInterval(intervalMs, TimeUnit.MILLISECONDS)
                .build();
            log.info("apply sticky partitioner with interval {} ms for client.id {}", intervalMs, clientId);
        } else {
            partitioner = new DefaultPartitioner();
        }

        this.metadataWaitingQueuePolicy = new MetadataWaitingQueuePolicy(
                metadataWaitingQueueSize <= 0 ? 10000 : metadataWaitingQueueSize,
                blockOnMetadataQueueFull);
        this.keyTopicMap = keyTopicMap != null ? keyTopicMap : Maps.<String, String>newHashMap();

        this.props = new Properties();
        if(etcProps != null) {
            this.props.putAll(etcProps);
        }
        this.props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        this.props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        if(StringUtils.isNotBlank(vipAddress)) {
            List<String> hostPortList = extractBootstrapServers(vipAddress, discoveryClient);
            // only override if we found UP brokers from discovery client
            if(hostPortList.isEmpty()) {
                log.warn("cannot find UP brokers for vipAddress: {}", vipAddress);
            } else {
                final String vipBootstrapServers = StringUtils.join(hostPortList, ",");
                this.props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, vipBootstrapServers);
                log.info("set {} bootstrap servers: {}", hostPortList.size(), vipBootstrapServers);
            }
        }
        setServoReporter();
    }

    private List<String> extractBootstrapServers(final String vipAddress, final DiscoveryClient discoveryClient) {
        int port = 7101;
        int sepIndex = vipAddress.indexOf(":");
        if(sepIndex > 0 && sepIndex < vipAddress.length() - 1) {
            // vipAddress contains port number
            try {
                port = Integer.valueOf(vipAddress.substring(sepIndex+1));
            } catch (NumberFormatException e) {
                log.error("fall back to port 7101 because vipAddress contains invalid port string: {}", vipAddress);
            }
        } else {
            log.info("use default port 7101 because vipAddress doesn't contain port number: {}", vipAddress);
        }
        // Allen mentioned that discovery client already shuffled/randomized the list
        List<InstanceInfo> instanceInfoList = discoveryClient.getInstancesByVipAddress(vipAddress, false);
        // extract UP instances
        List<String> hostPortList = new ArrayList<String>(instanceInfoList.size());
        for(InstanceInfo instanceInfo : instanceInfoList) {
            if(instanceInfo.getStatus() == InstanceInfo.InstanceStatus.UP) {
                hostPortList.add(String.format("%s:%d", instanceInfo.getHostName(), port));
            }
        }
        return hostPortList;
    }

    private String getRoutingKey(final MessageContainer message) {
        String routingKey = message.getRoutingKey();
        Preconditions.checkArgument(routingKey != null);
        if(normalizeRoutingKey) {
            routingKey = routingKey.toLowerCase();
        }
        return routingKey;
    }

    /**
     * we shouldn't need this hack after moving to 0.8.2.0
     */
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

        if (metadataFetchedTopicSet.contains(getRoutingKey(message))) {
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
        final String topic = getRoutingKey(message);
        byte[] key = null;
        if (!keyTopicMap.isEmpty() && keyTopicMap.get(topic) != null) {
            try {
                Map<String, Object> msgMap = message.getEntity(new TypeReference<Map<String, Object>>() {
                });
                Object keyField = msgMap.get(keyTopicMap.get(topic));
                if (keyField != null) {
                    key = keyField.toString().getBytes();
                }
            } catch (Exception e) {
                log.error("Exception on getting key field: " + e.getMessage());
            }
        }

        try {
            Integer part = partitioner.partition(topic, key, producer.partitionsFor(topic));
            producer.send(
                new ProducerRecord(topic, part, key, message.getMessage().getPayload()),
                new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            log.error("Exception while sending", e);
                            DynamicCounter.increment(
                                    MonitorConfig
                                            .builder("failedRecord")
                                            .withTag(TagKey.ROUTING_KEY, topic)
                                            .build());
                            droppedRecords.incrementAndGet();
                            runRecordCounterListener();
                        } else {
                            DynamicCounter.increment(
                                    MonitorConfig
                                            .builder("sentRecord")
                                            .withTag(TagKey.ROUTING_KEY, topic)
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
                .withTag(TagKey.ROUTING_KEY, getRoutingKey(message))
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
                                    if (!metadataFetchedTopicSet.contains(getRoutingKey(message))) {
                                        producer.partitionsFor(getRoutingKey(message));
                                        metadataFetchedTopicSet.add(getRoutingKey(message));
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

    @VisibleForTesting
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    @VisibleForTesting
    public String getVipAddress() {
        return vipAddress;
    }

    @VisibleForTesting
    public Properties getProperties() {
        return props;
    }

    @VisibleForTesting
    public Partitioner getPartitioner() {
        return partitioner;
    }
}
