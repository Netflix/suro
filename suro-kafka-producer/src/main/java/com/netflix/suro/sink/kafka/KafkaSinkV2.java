/*
 * Copyright 2014 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
import com.netflix.suro.sink.QueuedSink;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.ThreadPoolQueuedSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

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

    private final KafkaProducer<byte[], byte[]> producer;
    private long msgId = 0;
    private AtomicLong receivedCount = new AtomicLong(0);
    private AtomicLong sentCount = new AtomicLong(0);
    private AtomicLong sentByteCount = new AtomicLong(0);
    /** number of times a message send failed without retrying */
    private AtomicLong droppedCount = new AtomicLong(0);
    /** number of times a message send failed but was requeued */
    private AtomicLong requeuedCount = new AtomicLong(0);

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
            @JsonProperty("jobTimeout") long jobTimeout,
            @JsonProperty("pauseOnLongQueue") boolean pauseOnLongQueue
    ) {
        super(jobQueueSize, corePoolSize, maxPoolSize, jobTimeout,
                KafkaSink.class.getSimpleName() + "-" + clientId);

        Preconditions.checkNotNull(bootstrapServers);
        Preconditions.checkNotNull(clientId);

        this.clientId = clientId;
        initialize(
                "kafka_" + clientId,
                queue4Sink == null ? new MemoryQueue4Sink(10000) : queue4Sink,
                batchSize,
                batchTimeout,
                pauseOnLongQueue);

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

        producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());

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
                QueuedSink.log.error("Exception on getting key field: " + e.getMessage());
            }
        }
        QueuedSink.log.trace( "KafkaSink writeTo()" );
        receivedCount.incrementAndGet();
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
        QueuedSink.log.trace( "KafkaSink write() with {} messages", msgList.size() );
        // prepare "final" copies of the messages to be used in the anonymous class below
        final ArrayList<SuroKeyedMessage> msgCopies = 
                new ArrayList<SuroKeyedMessage>( msgList.size() );
        for( Message m : msgList ){
            SuroKeyedMessage sKeyedMsg = (SuroKeyedMessage) m;
            msgCopies.add( new SuroKeyedMessage( sKeyedMsg.getKey(), 
                                                 new Message( m.getRoutingKey(), m.getPayload() )));
        }

        // The new KafkaProducer does not have interface for sending multiple messages,
        // so we loop and create lots of Runnables -- this seems inefficient, but the alternative
        // has its own problems.  If we create one "big Runnable" that loops over messages we'll
        // drain the queue4sink too quickly -- all the messages will be queued in the in-memory 
        // job queue storing the Runnables.
        for( final SuroKeyedMessage m : msgCopies ) {
            senders.submit(new Runnable() {
                @Override
                public void run() {
                    String topic = m.getRoutingKey();

                    // calculate the kafka partition, with backward compatibility with old kafka producer
                    int numPartitions = producer.partitionsFor(topic).size();
                    int partition = Math.abs((int)(m.getKey() ^ (m.getKey() >>> 32))) % numPartitions;

                    ProducerRecord r = new ProducerRecord( topic,
                                                           partition,
                                                           null, // don't store the key
                                                           m.getPayload() );
                    QueuedSink.log.trace( "Will send message to Kafka" );
                    long startTimeMs = System.currentTimeMillis();
                    // send
                    Future<RecordMetadata> responseFtr = producer.send( r );
                    QueuedSink.log.trace( "Started aysnc producer" );
                    boolean failure = true;
                    boolean retry = true;
                    if( responseFtr.isCancelled() ){
                        QueuedSink.log.warn( "Kafka producer request was cancelled" );
                        // we assume that cancelled requests should not be retried.
                        retry = false;
                    }
                    try {
                        // wait for request to finish
                        RecordMetadata response = responseFtr.get();
                        if( response.topic() == null ){
                            QueuedSink.log.warn( "Kafka producer got null topic in response" );
                        }
                        sentCount.incrementAndGet();
                        sentByteCount.addAndGet( m.getPayload().length );
                        failure = false;
                        retry = false;
                    } catch (InterruptedException e) {
                        // Assume that Interrupted means we're trying to shutdown so don't retry
                        QueuedSink.log.warn( "Caught InterruptedException: "+ e );
                        retry = false;
                    } catch( UnknownTopicOrPartitionException e ){
                        QueuedSink.log.warn( "Caught UnknownTopicOrPartitionException for topic: " + m.getRoutingKey()
                                  +" This may be simply because KafkaProducer does not yet have information about the brokers."
                                  +" Request will be retried.");
                    } catch (ExecutionException e) {
                        QueuedSink.log.warn( "Caught ExecutionException: "+ e );
                    } catch (Exception e){
                        QueuedSink.log.warn( "Caught Exception: "+e );
                    }
                    long durationMs = System.currentTimeMillis() - startTimeMs;

                    if (failure){
                        QueuedSink.log.warn( "Kafka producer send failed after {} milliseconds", durationMs );
                        requeuedCount.incrementAndGet();
                        if( retry ){
                            enqueue( m );
                        }else{
                            QueuedSink.log.info("Dropped message");
                            droppedCount.incrementAndGet();
                        }
                    } else{
                        QueuedSink.log.trace( "Kafka producer send succeeded after {} milliseconds", durationMs );
                    }
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
        Map<MetricName,? extends Metric> metrics = producer.metrics();
        StringBuilder sb = new StringBuilder();
        // add kafka producer stats, which are rates
        for( Map.Entry<MetricName,? extends Metric> e : metrics.entrySet() ){
            sb.append("kafka.").append(e.getKey()).append(": ").append(e.getValue().value()).append('\n');
        }
        // also report our counters
        sb.append("messages-in-queue4sink: ").append( this.queue4Sink.size() ).append('\n');
        sb.append("queued-jobs: ").append( this.jobQueue.size() ).append('\n');
        sb.append("active-threads: ").append( this.senders.getActiveCount() ).append('\n');
        sb.append("received-messages: ").append( this.receivedCount.get() ).append('\n');
        sb.append("sent-messages: ").append( this.sentCount.get() ).append('\n');
        sb.append("sent-bytes: ").append( this.sentByteCount.get() ).append('\n');
        sb.append("dropped-messages: ").append( this.droppedCount.get() ).append('\n');
        sb.append("requeued-messages: ").append( this.requeuedCount.get() ).append('\n');

        return sb.toString();
    }
}