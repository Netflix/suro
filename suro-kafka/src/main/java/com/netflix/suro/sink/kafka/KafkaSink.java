package com.netflix.suro.sink.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

    private final ThreadPoolExecutor senders;
    private final ArrayBlockingQueue<Runnable> jobQueue;
    private final long jobTimeout;

    @Monitor(name = "jobQueueSize", type = DataSourceType.GAUGE)
    public long getJobQueueSize() {
        return jobQueue.size();
    }

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
            @JsonProperty("keyTopicMap") Map<String, String> keyTopicMap,
            @JsonProperty("jobQueueSize") int jobQueueSize,
            @JsonProperty("corePoolSize") int corePoolSize,
            @JsonProperty("maxPoolSize") int maxPoolSize,
            @JsonProperty("jobTimeout") long jobTimeout
    ) {
        Preconditions.checkNotNull(brokerList);
        Preconditions.checkNotNull(acks);
        Preconditions.checkNotNull(clientId);

        this.clientId = clientId;
        initialize(queue4Sink == null ? new MemoryQueue4Sink(10000) : queue4Sink, batchSize, batchTimeout);

        jobQueue = new ArrayBlockingQueue<Runnable>(jobQueueSize == 0 ? 1000 : jobQueueSize) {
            @Override
            public boolean offer(Runnable runnable) {
                try {
                    put(runnable); // not to reject the task, slowing down
                } catch (InterruptedException e) {
                    // do nothing
                }
                return true;
            }
        };
        senders = new ThreadPoolExecutor(
                corePoolSize == 0 ? 3 : corePoolSize,
                maxPoolSize == 0 ? 10 : maxPoolSize,
                10, TimeUnit.SECONDS,
                jobQueue);
        this.jobTimeout = jobTimeout;

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

        Monitors.registerObject(KafkaSink.class.getSimpleName() + "-" + clientId, this);
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
        senders.shutdown();
        try {
            senders.awaitTermination(jobTimeout == 0 ? Long.MAX_VALUE : jobTimeout, TimeUnit.MILLISECONDS);
            if (!senders.isTerminated()) {
                log.error("KafkaSink not terminated gracefully");
            }
        } catch (InterruptedException e) {
            log.error("Interrupted: " + e.getMessage());
        }

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

    protected void send(final List<Message> msgList) {
        final List<KeyedMessage<Long, byte[]>> kafkaMsgList = new ArrayList<KeyedMessage<Long, byte[]>>();
        for (Message m : msgList) {
            SuroKeyedMessage keyedMessage = (SuroKeyedMessage) m;
            kafkaMsgList.add(new KeyedMessage<Long, byte[]>(
                    keyedMessage.getRoutingKey(),
                    keyedMessage.getKey(),
                    keyedMessage.getPayload()));
        }

        senders.submit(new Runnable() {
            @Override
            public void run() {
                producer.send(kafkaMsgList);
            }
        });
    }
}
