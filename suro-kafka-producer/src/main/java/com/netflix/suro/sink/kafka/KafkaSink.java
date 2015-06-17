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
import com.netflix.suro.message.StringMessage;
import com.netflix.suro.sink.Sink;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action3;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka 0.8.2 Sink
 *
 * @author jbae
 */
public class KafkaSink implements Sink {
    private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);

    public final static String TYPE = "kafka";

    private final static MessageContainer SHUTDOWN_POISON_MSG = new StringMessage("suro-KafkaSink-shutdownMsg-routingKey",
            "suro-KafkaSink-shutdownMsg-body");

    private final boolean normalizeRoutingKey;
    private final String clientId;
    private final String bootstrapServers;
    private final String vipAddress;
    private final String partitionerSelector;
    private final int stickyInterval;
    private final int metadataWaitingQueueSize;
    private final Map<String, String> keyTopicMap;

    private final Partitioner partitioner;
    private final Properties props;
    private final Set<String> metadataFetchedTopicSet;
    private final BlockingQueue<MessageContainer> metadataWaitingQueue;
    private final ExecutorService executor;

    private volatile boolean isOpened = false;
    private volatile KafkaProducer<byte[], byte[]> producer;

    private AtomicLong queuedRecords = new AtomicLong(0);
    private AtomicLong sentRecords = new AtomicLong(0);
    private AtomicLong droppedRecords = new AtomicLong(0);
    private volatile Action3 recordCounterListener;
    public void setRecordCounterListener(Action3 action) {
        this.recordCounterListener = action;
    }

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
        this.clientId = clientId;
        this.bootstrapServers = bootstrapServers;
        this.vipAddress = vipAddress;
        this.partitionerSelector = partitionerSelector;
        this.stickyInterval = stickyInterval;
        this.metadataWaitingQueueSize = metadataWaitingQueueSize > 0 ? metadataWaitingQueueSize : 1024;
        this.keyTopicMap = keyTopicMap != null ? keyTopicMap : Maps.<String, String>newHashMap();

        if(StickyPartitioner.NAME.equals(partitionerSelector)) {
            int intervalMs = stickyInterval > 0 ? stickyInterval : 1000;
            partitioner = StickyPartitioner.builder()
                .withInterval(intervalMs, TimeUnit.MILLISECONDS)
                .build();
            log.info("apply sticky partitioner with interval {} ms for client.id {}", intervalMs, clientId);
        } else {
            partitioner = new DefaultPartitioner();
        }

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
        props.setProperty(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, ServoReporter.class.getName());

        this.metadataFetchedTopicSet = new CopyOnWriteArraySet<String>();
        this.metadataWaitingQueue = new ArrayBlockingQueue<MessageContainer>(this.metadataWaitingQueueSize);
        this.executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("KafkaSink-MetadataFetcher-%d").build());
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
            // with potential cluster size of over 500,
            // it is unnecessary to list all hosts for bootstrap.servers.
            // again, it is only for bootstrap purpose
            if(hostPortList.size() >= 30) {
                break;
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

    @Override
    public void writeTo(final MessageContainer message) {
        queuedRecords.incrementAndGet();
        if(!isOpened) {
            dropMessage(getRoutingKey(message), "sinkNotOpened");
            return;
        }
        runRecordCounterListener();

        final String routingKey = getRoutingKey(message);
        if (metadataFetchedTopicSet.contains(routingKey)) {
            sendMessage(message);
        } else {
            DynamicCounter.increment(
                    MonitorConfig
                            .builder("metadataNotFetched")
                            .withTag(TagKey.ROUTING_KEY, routingKey)
                            .withTag(TagKey.CLIENT_ID, clientId)
                            .build());
            if(!metadataWaitingQueue.offer(message)) {
                dropMessage(getRoutingKey(message), "metadataWaitingQueueFull");
            }
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
                log.debug("Failed to extract key field: " + keyTopicMap.get(topic), e);
                DynamicCounter.increment(
                        MonitorConfig
                                .builder("extractPartitionKeyError")
                                .withTag(TagKey.ROUTING_KEY, topic)
                                .withTag(TagKey.CLIENT_ID, clientId)
                                .build());
                // just increment a counter and continue the send just like without key
            }
        }

        final Integer part;
        try {
            part = partitioner.partition(topic, key, producer.partitionsFor(topic));
        } catch(Exception e) {
            log.debug("partitioner failure: " + topic, e);
            dropMessage(topic, "partitionerError");
            // abort send
            return;
        }

        try {
            DynamicCounter.increment(
                    MonitorConfig
                            .builder("attemptRecord")
                            .withTag(TagKey.ROUTING_KEY, topic)
                            .withTag(TagKey.CLIENT_ID, clientId)
                            .build());
            producer.send(
                new ProducerRecord(topic, part, key, message.getMessage().getPayload()),
                new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            log.debug("Failed to send: " + topic, e);
                            String droppedReason = "sendError";
                            if(e instanceof RecordTooLargeException) {
                                droppedReason = "recordTooLarge";
                            }
                            dropMessage(topic, droppedReason);
                            runRecordCounterListener();
                        } else {
                            DynamicCounter.increment(
                                    MonitorConfig
                                            .builder("sentRecord")
                                            .withTag(TagKey.ROUTING_KEY, topic)
                                            .withTag(TagKey.CLIENT_ID, clientId)
                                            .build());
                            sentRecords.incrementAndGet();
                            runRecordCounterListener();
                        }
                    }
                });
        } catch (Exception e) {
            log.debug("Failed to submit send: " + topic, e);
            dropMessage(topic, "submitSendError");
        }
    }

    private void dropMessage(final String routingKey, final String reason) {
        DynamicCounter.increment(
            MonitorConfig
                .builder("droppedRecord")
                .withTag(TagKey.ROUTING_KEY, routingKey)
                .withTag(TagKey.DROPPED_REASON, reason)
                .withTag(TagKey.CLIENT_ID, clientId)
                .build());
        droppedRecords.incrementAndGet();
        runRecordCounterListener();
    }

    @Override
    public void open() {
        producer = new KafkaProducer<byte[], byte[]>(props, new ByteArraySerializer(), new ByteArraySerializer());
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
                    final String topic = getRoutingKey(message);
                    try {
                        if (!metadataFetchedTopicSet.contains(topic)) {
                            producer.partitionsFor(topic);
                            metadataFetchedTopicSet.add(topic);
                        }
                        sendMessage(message);
                    } catch(Throwable t) {
                        log.error("failed to get metadata: " + topic, t);
                        // try to put back to the queue if there is still space
                        if(!metadataWaitingQueue.offer(message)) {
                            dropMessage(getRoutingKey(message), "metadataWaitingQueueFull");
                        }
                    }
                }
            }
        });
        isOpened = true;
    }

    @Override
    public void close() {
        isOpened = false;
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
    public boolean isOpened() {
        return isOpened;
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
