package com.netflix.suro.input.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.message.MessageSetReader;
import com.netflix.suro.message.StringMessage;
import com.netflix.suro.routing.MessageRouter;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.kafka.KafkaServerExternalResource;
import com.netflix.suro.sink.kafka.KafkaSink;
import com.netflix.suro.sink.kafka.TestKafkaSink;
import com.netflix.suro.sink.kafka.ZkExternalResource;
import kafka.admin.TopicCommand;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", TOPIC_NAME,
                        "--replication-factor", "2", "--partitions", "1"}));

        ObjectMapper jsonMapper = new DefaultObjectMapper();

        sendKafkaMessage(jsonMapper);

        final CountDownLatch latch = new CountDownLatch(2);

        MessageRouter router = mock(MessageRouter.class);
        doAnswer(new Answer() {

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                latch.countDown();
                return null;
            }
        }).when(router).process(any(MessageContainer.class));

        Properties properties = new Properties();
        properties.setProperty("group.id", "testkafkaconsumer");
        properties.setProperty("zookeeper.connect", zk.getConnectionString());
        properties.setProperty("auto.offset.reset", "smallest");

        KafkaConsumer consumer = null;
        try {
            consumer = new KafkaConsumer(properties, TOPIC_NAME, router, jsonMapper);
            fail("should have failed without timeout");
        } catch (Exception e) {
            // do nothing
        }

        properties.setProperty("consumer.timeout.ms", "1000");
        consumer = new KafkaConsumer(properties, TOPIC_NAME, router, jsonMapper);

        consumer.start();
        latch.await(500000, TimeUnit.MILLISECONDS);

        ArgumentCaptor<MessageContainer> msgContainers = ArgumentCaptor.forClass(MessageContainer.class);
        verify(router, times(2)).process(msgContainers.capture());
        for (MessageContainer container : msgContainers.getAllValues()) {
            assertEquals(container.getRoutingKey(), TOPIC_NAME);
            assertTrue(container.getEntity(String.class).startsWith("testMessage"));
        }
        consumer.stopTakingTraffic();

        final CountDownLatch latch1 = new CountDownLatch(2);
        doAnswer(new Answer() {

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                latch1.countDown();
                return null;
            }
        }).when(router).process(any(MessageContainer.class));

        sendKafkaMessage(jsonMapper);
        latch1.await(5000, TimeUnit.MILLISECONDS);
        assertEquals(latch1.getCount(), 2); // not processed due to stop

        consumer.startTakingTraffic();
        latch1.await(5000, TimeUnit.MILLISECONDS);
        assertEquals(latch1.getCount(), 0);
        verify(router, times(4)).process(msgContainers.capture());
        for (MessageContainer container : msgContainers.getAllValues()) {
            assertEquals(container.getRoutingKey(), TOPIC_NAME);
            assertTrue(container.getEntity(String.class).startsWith("testMessage"));
        }
    }

    private void sendKafkaMessage(ObjectMapper jsonMapper) throws java.io.IOException, InterruptedException {
        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"metadata.broker.list\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"request.required.acks\": 1\n" +
                "}";

        jsonMapper.registerSubtypes(new NamedType(KafkaSink.class, "kafka"));
        KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        sink.open();
        Iterator<Message> msgIterator = new MessageSetReader(TestKafkaSink.createMessageSet(TOPIC_NAME, 2)).iterator();
        while (msgIterator.hasNext()) {
            sink.writeTo(new StringMessage(msgIterator.next()));
        }
        sink.close();
    }
}
