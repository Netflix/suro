package com.netflix.suro.sink.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.queue.MemoryQueue4Sink;
import com.netflix.suro.queue.MessageQueue4Sink;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.ThreadPoolQueuedSink;

import kafka.metrics.KafkaMetricsReporter$;
import kafka.producer.*;
import kafka.utils.VerifiableProperties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Kafka 0.8.2 Sink, using new Java-native producer, rather than Scala produer.
 * Requests are re-queued indefinitely if they fail.
 * 
 * The configuration parameters for the new kafka producer are listed in:
 * http://kafka.apache.org/documentation.html#newproducerconfigs
 *
 * @author jbae
 * @author starzia
 */
public class KafkaSinkV2 extends ThreadPoolQueuedSink implements Sink {
    public final static String TYPE = "KafkaV2";

    private String clientId;
    private final Map<String, String> keyTopicMap;

    private final KafkaProducer producer;
    private long msgId = 0;

    @JsonCreator
    public KafkaSinkV2(
            @JsonProperty("queue4Sink") MessageQueue4Sink queue4Sink,
            @JsonProperty("client.id") String clientId,
            @JsonProperty("metadata.broker.list") String bootstrapServers,
            @JsonProperty("compression.codec") String codec,
            @JsonProperty("send.buffer.bytes") int sendBufferBytes,
            @JsonProperty("batchSize") int batchSize,
            @JsonProperty("batchTimeout") int batchTimeout,
            @JsonProperty("request.timeout.ms") int requestTimeout,
            @JsonProperty("kafka.etc") Properties etcProps,
            @JsonProperty("keyTopicMap") Map<String, String> keyTopicMap,
            @JsonProperty("jobQueueSize") int jobQueueSize,
            @JsonProperty("corePoolSize") int corePoolSize,
            @JsonProperty("maxPoolSize") int maxPoolSize,
            @JsonProperty("jobTimeout") long jobTimeout
    ) {
        super(jobQueueSize, corePoolSize, maxPoolSize, jobTimeout,
                KafkaSink.class.getSimpleName() + "-" + clientId);

        Preconditions.checkNotNull(bootstrapServers);
        Preconditions.checkNotNull(clientId);

        this.clientId = clientId;
        initialize("kafka_" + clientId, queue4Sink == null ? new MemoryQueue4Sink(10000) : queue4Sink, batchSize, batchTimeout);

        Properties props = new Properties();
        props.put("client.id", clientId);
        // metadata.broker.list was renamed to bootstrap.servers in the new kafka producer
        props.put("bootstrap.servers", bootstrapServers);
        if (codec != null) {
            props.put("compression.codec", codec);
        }
        if (sendBufferBytes > 0) {
            props.put("send.buffer.bytes", Integer.toString(sendBufferBytes));
        }
        if (requestTimeout > 0) {
            props.put("request.timeout.ms", Integer.toString(requestTimeout));
        }

        if (etcProps != null) {
            props.putAll(etcProps);
        }

        this.keyTopicMap = keyTopicMap != null ? keyTopicMap : Maps.<String, String>newHashMap();

        producer = new KafkaProducer( props );
        KafkaMetricsReporter$.MODULE$.startReporters(new VerifiableProperties(props));

        Monitors.registerObject(clientId, this);
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
        log.trace( "KafkaSink writeTo()" );
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
        log.trace( "KafkaSink write() with {} messages", msgList.size() );
        // prepare "final" copies of the messages to be used in the anonymous class below
        final ArrayList<Message> msgCopies = new ArrayList<Message>( msgList.size() );
        for( Message m : msgList ){
            msgCopies.add( new Message( m.getRoutingKey(), m.getPayload() ) );
        }

        // :( The new KafkaProducer does not have interface for sending multiple messages
        for( final Message m : msgCopies ) {
            senders.submit(new Runnable() {
                @Override
                public void run() {
                    // TODO: somehow use keyForTopic map in SinkConfig
                    ProducerRecord r = new ProducerRecord( m.getRoutingKey(), // Kafka topic
                                                           m.getPayload() );  // Kafka payload
                    log.trace( "Will send message to Kafka" );
                    long startTimeMs = System.currentTimeMillis();
                    Future<RecordMetadata> responseFtr = producer.send( r );
                    log.trace( "Started aysnc producer" );
                    boolean success = true;
                    if( responseFtr.isCancelled() ){
                        log.warn( "Kafka producer request was cancelled" );
                        // note that we do not set success = false because we assume that cancelled
                        // requests should not be retried.
                    }
                    try {
                        // wait for request to finish
                        RecordMetadata response = responseFtr.get();
                        if( response.topic() == null ){
                            log.warn( "Kafka producer got null topic in response" );
                        }
                    }catch (InterruptedException e) {
                        // ???: I don't know whether we should re-queue the request here.
                        // For now, assume that Interruption does not mean failure
                        log.warn( "Caught InterruptedException: "+ e );
                    }catch( UnknownTopicOrPartitionException e ){
                        log.warn( "Caught UnknownTopicOrPartitionException for topic: " + m.getRoutingKey()
                                  +"\tThis may be simply because KafkaProducer does not yet have information about the brokers."
                                  +"\tRequest will be retried.");
                        success = false;
                    }catch (ExecutionException e) {
                        log.warn( "Caught ExecutionException: "+ e );
                        success = false;
                    }catch (Exception e){
                        log.warn( "Caught Exception: "+e );
                        success = false;
                    }
                    long durationMs = System.currentTimeMillis() - startTimeMs;
                    if( !success ){
                        // sending the request failed.
                        log.warn( "Kafka producer send failed after {} milliseconds", durationMs );
                        enqueue( m );
                    }else{
                        log.trace( "Kafka producer send succeeded after {} milliseconds", durationMs );
                    }
                    // TODO: do we need to do something to set statistics queried in getStat(), below?
                }
            });
        }
    }

    @Override
    protected void innerClose() {
        super.innerClose();

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
}