package com.netflix.suro;

import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.netflix.client.http.HttpRequest;
import com.netflix.client.http.HttpResponse;
import com.netflix.niws.client.http.RestClient;
import com.netflix.suro.input.kafka.KafkaConsumer;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.DefaultMessageContainer;
import com.netflix.suro.message.Message;
import com.netflix.suro.routing.MessageRouter;
import com.netflix.suro.routing.RoutingMap;
import com.netflix.suro.sink.QueuedSink;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.SinkManager;
import com.netflix.suro.sink.elasticsearch.ElasticSearchSink;
import com.netflix.suro.sink.kafka.KafkaRetentionPartitioner;
import com.netflix.suro.sink.kafka.KafkaServerExternalResource;
import com.netflix.suro.sink.kafka.KafkaSink;
import com.netflix.suro.sink.kafka.ZkExternalResource;
import kafka.admin.TopicCommand;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class TestPauseOnLongQueueKafkaConsumer {
    public static ZkExternalResource zk = new ZkExternalResource();
    public static KafkaServerExternalResource kafkaServer = new KafkaServerExternalResource(zk);

    @ClassRule
    public static TestRule chain = RuleChain
            .outerRule(zk)
            .around(kafkaServer);

    private static final String TOPIC_NAME = "tpolq_kafka";

    private final CountDownLatch latch = new CountDownLatch(1);

    @Test
    public void test() throws Exception {
        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", TOPIC_NAME,
                        "--replication-factor", "2", "--partitions", "1"}));

        final ObjectMapper jsonMapper = new DefaultObjectMapper();
        jsonMapper.setInjectableValues(new InjectableValues() {
            @Override
            public Object findInjectableValue(Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance) {
                if (valueId.equals(KafkaRetentionPartitioner.class.getName())) {
                    return new KafkaRetentionPartitioner();
                } else {
                    return null;
                }
            }
        });

        final KafkaSink kafkaSink = createKafkaProducer(jsonMapper, kafkaServer.getBrokerListStr());

        RestClient client = mock(RestClient.class);

        final ElasticSearchSink sink = new ElasticSearchSink(
                "tpolq",
                null,
                10,
                1000,
                Lists.newArrayList("localhost:9200"),
                null,
                0,0,0,0,0,
                null,
                false,
                jsonMapper,
                client
        );

        doAnswer(new Answer() {

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final HttpRequest bulk = (HttpRequest) invocation.getArguments()[0];
                int numRecords = bulk.getEntity().toString().split("\n").length / 2;
                latch.await();

                HttpResponse result = mock(HttpResponse.class);
                doReturn(200).when(result).getStatus();
                List list = new ArrayList();
                for (int i = 0; i < numRecords; ++i) {
                    list.add(new ImmutableMap.Builder<>().put("create",
                        new ImmutableMap.Builder<>().put("status", 200).build()).build());
                }

                doReturn(new StringInputStream(
                    jsonMapper.writeValueAsString(
                        new ImmutableMap.Builder<>()
                            .put("took", 1000)
                            .put("items", list)
                            .build()))).when(result).getInputStream();

                return result;
            }
        }).when(client).executeWithLoadBalancer(any(HttpRequest.class));

        sink.open();

        RoutingMap map = new RoutingMap();
        map.set(new ImmutableMap.Builder<String, RoutingMap.RoutingInfo>()
                .put(TOPIC_NAME, new RoutingMap.RoutingInfo(Lists.newArrayList(new RoutingMap.Route("es", null, null)), null))
                .build());

        SinkManager sinks = new SinkManager();
        sinks.initialSet(new ImmutableMap.Builder<String, Sink>()
                .put("es", sink).build());

        MessageRouter router = new MessageRouter(map, sinks, jsonMapper);

        Properties properties = new Properties();
        properties.setProperty("group.id", "testkafkaconsumer");
        properties.setProperty("zookeeper.connect", zk.getConnectionString());
        properties.setProperty("auto.offset.reset", "smallest");

        properties.setProperty("consumer.timeout.ms", "1000");
        KafkaConsumer consumer = new KafkaConsumer(properties, TOPIC_NAME, 1, router, jsonMapper);

        consumer.start();

        // set the pause threshold to 100
        QueuedSink.MAX_PENDING_MESSAGES_TO_PAUSE = 10;

        Thread t = createProducerThread(jsonMapper, kafkaSink, TOPIC_NAME);

        // wait until queue's is full over the threshold
        int count = 0;
        while (count < 3) {
            System.out.println("pending messages:" + sink.getNumOfPendingMessages());
            if (sink.getNumOfPendingMessages() >= QueuedSink.MAX_PENDING_MESSAGES_TO_PAUSE) {
                ++count;
            }
            Thread.sleep(1000);
        }

        // get the number of pending messages for 10 seconds
        ArrayList<Integer> countList = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            countList.add((int) sink.getNumOfPendingMessages());
            Thread.sleep(1000);
        }

        for (int i = 6; i < 9; ++i) {
            assertEquals(countList.get(i), countList.get(i + 1), 5);
        }

        latch.countDown();

        run.set(false);
        t.join();

        consumer.shutdown();

        sink.close();

        assertEquals(sink.getNumOfPendingMessages(), 0);
    }

    private KafkaSink createKafkaProducer(ObjectMapper jsonMapper, String brokerListStr) throws java.io.IOException {
        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"" + brokerListStr + "\",\n" +
                "    \"acks\": 1\n" +
                "}";

        jsonMapper.registerSubtypes(new NamedType(KafkaSink.class, "kafka"));
        final KafkaSink kafkaSink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        kafkaSink.open();
        return kafkaSink;
    }

    private final AtomicBoolean run = new AtomicBoolean(true);

    private Thread createProducerThread(final ObjectMapper jsonMapper, final KafkaSink kafkaSink, final String topicName) {
        Thread t = new Thread(new Runnable() {
            private final RateLimiter rateLimiter = RateLimiter.create(10);

            @Override
            public void run() {
                while (run.get()) {
                    rateLimiter.acquire();
                    try {
                        kafkaSink.writeTo(new DefaultMessageContainer(
                                new Message(
                                        topicName,
                                        jsonMapper.writeValueAsBytes(
                                                new ImmutableMap.Builder<String, Object>()
                                                        .put("f1", "v1")
                                                        .put("f2", "v2")
                                                        .build())),
                                jsonMapper));
                    } catch (Exception e) {
                        fail();
                    }
                }

                kafkaSink.close();
            }
        });
        t.start();

        return t;
    }
}
