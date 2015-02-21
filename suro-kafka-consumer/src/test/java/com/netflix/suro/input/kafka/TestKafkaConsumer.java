package com.netflix.suro.input.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.netflix.suro.input.SuroInput;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.routing.MessageRouter;
import com.netflix.suro.sink.kafka.KafkaServerExternalResource;
import com.netflix.suro.sink.kafka.ZkExternalResource;
import kafka.admin.TopicCommand;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class TestKafkaConsumer {
    public static ZkExternalResource zk = new ZkExternalResource();
    public static KafkaServerExternalResource kafkaServer = new KafkaServerExternalResource(zk);

    @ClassRule
    public static TestRule chain = RuleChain
            .outerRule(zk)
            .around(kafkaServer);

    private static final String TOPIC_NAME = "testkafkaconsumer";
    @Test
    public void test() throws Exception {
        int numPartitions = 6;
        int messageCount = 10;
        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", TOPIC_NAME,
                        "--replication-factor", "2", "--partitions", Integer.toString(numPartitions)}));

        ObjectMapper jsonMapper = new DefaultObjectMapper();

        sendKafkaMessage(kafkaServer.getBrokerListStr(), TOPIC_NAME, numPartitions, messageCount);

        final CountDownLatch latch = new CountDownLatch(numPartitions * messageCount);

        MessageRouter router = mock(MessageRouter.class);
        doAnswer(new Answer() {

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                latch.countDown();
                return null;
            }
        }).when(router).process(any(SuroInput.class), any(MessageContainer.class));

        Properties properties = new Properties();
        properties.setProperty("group.id", "testkafkaconsumer");
        properties.setProperty("zookeeper.connect", zk.getConnectionString());
        properties.setProperty("auto.offset.reset", "smallest");

        try {
            new KafkaConsumer(properties, TOPIC_NAME, numPartitions, router, jsonMapper);
            fail("should have failed without timeout");
        } catch (Exception e) {
            // do nothing
        }

        properties.setProperty("consumer.timeout.ms", "1000");
        KafkaConsumer consumer = new KafkaConsumer(properties, TOPIC_NAME, numPartitions, router, jsonMapper);
        KafkaConsumer.MAX_PAUSE = 10000; // for testing

        consumer.start();
        latch.await(1000 * 5, TimeUnit.MILLISECONDS);

        ArgumentCaptor<MessageContainer> msgContainers = ArgumentCaptor.forClass(MessageContainer.class);
        verify(router, times(numPartitions * messageCount)).process(any(SuroInput.class), msgContainers.capture());
        for (MessageContainer container : msgContainers.getAllValues()) {
            assertEquals(container.getRoutingKey(), TOPIC_NAME);
            assertTrue(container.getEntity(String.class).startsWith("testMessage"));
        }

        final CountDownLatch latch1 = new CountDownLatch(numPartitions * messageCount);
        doAnswer(new Answer() {

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                latch1.countDown();
                return null;
            }
        }).when(router).process(any(SuroInput.class), any(MessageContainer.class));

        long pauseTime = 5000;
        consumer.setPause(pauseTime);
        long start = System.currentTimeMillis();

        sendKafkaMessage(kafkaServer.getBrokerListStr(), TOPIC_NAME, numPartitions, messageCount);

        latch1.await(1000 * 5 + pauseTime, TimeUnit.MILLISECONDS);
        long end = System.currentTimeMillis();
        assertTrue(end - start > pauseTime);

        msgContainers = ArgumentCaptor.forClass(MessageContainer.class);
        verify(router, times(numPartitions * messageCount * 2)).process(any(SuroInput.class), msgContainers.capture());

        for (MessageContainer container : msgContainers.getAllValues()) {
            assertEquals(container.getRoutingKey(), TOPIC_NAME);
            assertTrue(container.getEntity(String.class).startsWith("testMessage"));
        }

        consumer.shutdown();
    }

    public static void sendKafkaMessage(String brokerList, String topicName, int partitionCount, int messageCount) throws java.io.IOException, InterruptedException {
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>
                (new ImmutableMap.Builder<String, Object>()
                        .put("client.id", "kakasink")
                        .put("bootstrap.servers", brokerList).build(),
                    new ByteArraySerializer(), new ByteArraySerializer());
        for (int i = 0; i < messageCount; ++i) {
            for (int j = 0; j < partitionCount; ++j) {
                producer.send(new ProducerRecord(topicName, j, null, new String("testMessage1").getBytes()));
            }
        }
    }
}
