package com.netflix.suro.input.kafka;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.suro.input.SuroInput;
import com.netflix.suro.message.DefaultMessageContainer;
import com.netflix.suro.message.Message;
import com.netflix.suro.routing.MessageRouter;
import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaConsumer implements SuroInput {
    public static final String TYPE = "kafka";
    private static Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    protected final Properties consumerProps;
    private final String topic;
    private final MessageRouter router;
    private final ObjectMapper jsonMapper;

    private ConsumerConnector connector;
    private ExecutorService executor;
    private final int readers;
    private List<Future<?>> runners = new ArrayList<Future<?>>();
    private volatile boolean running = false;

    @JsonCreator
    public KafkaConsumer(
            @JsonProperty("consumerProps") Properties consumerProps,
            @JsonProperty("topic") String topic,
            @JsonProperty("readers") int readers,
            @JacksonInject MessageRouter router,
            @JacksonInject ObjectMapper jsonMapper
    ) {
        Preconditions.checkNotNull(consumerProps);
        Preconditions.checkNotNull(topic);
        Preconditions.checkNotNull(consumerProps.getProperty("group.id"));
        Preconditions.checkNotNull(consumerProps.getProperty("zookeeper.connect"));
        String timeoutStr = consumerProps.getProperty("consumer.timeout.ms");
        Preconditions.checkNotNull(timeoutStr);
        Preconditions.checkArgument(Long.parseLong(timeoutStr) > 0);

        this.consumerProps = consumerProps;
        this.topic = topic;
        this.readers = readers == 0 ? 1 : readers;
        this.router = router;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public String getId() {
        return topic + "-" + consumerProps.getProperty("group.id");
    }

    private AtomicLong pausedTime = new AtomicLong(0);

    public static long MAX_PAUSE = 1000; // not final for the test

    @Override
    public void start() throws Exception {
        executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("KafkaConsumer-%d").build());
        connector = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps));

        final Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(ImmutableMap.of(topic, readers));

        final List<KafkaStream<byte[], byte[]>> streamList = streams.get(topic);
        if (streamList == null) {
            throw new RuntimeException(topic + " is not valid");
        }

        running = true;
        for (KafkaStream<byte[], byte[]> stream : streamList) {
            final ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
            runners.add(
                    executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            while (running) {
                                try {
                                    long pause = Math.min(pausedTime.get(), MAX_PAUSE);
                                    if (pause > 0) {
                                        Thread.sleep(pause);
                                        pausedTime.set(0);
                                    }
                                    byte[] message = iterator.next().message();
                                    router.process(
                                            KafkaConsumer.this,
                                            new DefaultMessageContainer(new Message(topic, message), jsonMapper));
                                } catch (ConsumerTimeoutException timeoutException) {
                                    // do nothing
                                } catch (Exception e) {
                                    log.error("Exception on consuming kafka with topic: " + topic, e);
                                }
                            }
                        }
                    })
            );
        }
    }

    @Override
    public void shutdown() {
        stop();
        connector.shutdown();
    }

    @Override
    public void setPause(long ms) {
        pausedTime.addAndGet(ms);
    }

    private void stop() {
        running = false;
        try {
            for (Future<?> runner : runners) {
                runner.get();
            }
        } catch (InterruptedException e) {
            // do nothing
        } catch (ExecutionException e) {
            log.error("Exception on stopping the task", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof KafkaConsumer) {
            KafkaConsumer kafkaConsumer = (KafkaConsumer) o;
            boolean topicEquals = topic.equals(kafkaConsumer.topic);
            if (topicEquals) {
                return consumerProps.getProperty("group.id").equals(kafkaConsumer.consumerProps.getProperty("group.id"));
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return (getId()).hashCode();
    }
}
