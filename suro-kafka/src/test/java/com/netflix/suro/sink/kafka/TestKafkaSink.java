package com.netflix.suro.sink.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.*;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.thrift.TMessageSet;
import kafka.admin.TopicCommand;
import kafka.api.FetchRequestBuilder;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;
import kafka.server.KafkaConfig;
import kafka.utils.ZkUtils;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import scala.Option;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class TestKafkaSink {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    public static ZkExternalResource zk = new ZkExternalResource();
    public static KafkaServerExternalResource kafkaServer = new KafkaServerExternalResource(zk);

    @ClassRule
    public static TestRule chain = RuleChain
            .outerRule(zk)
            .around(kafkaServer);

    private static final String TOPIC_NAME = "routingKey";
    private static final String TOPIC_NAME_MULTITHREAD = "routingKeyMultithread";
    private static final String TOPIC_NAME_PARTITION_BY_KEY = "routingKey_partitionByKey";

    @Test
    public void testDefaultParameters() throws IOException {
        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", TOPIC_NAME,
                        "--replication-factor", "2", "--partitions", "1"}));
        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"metadata.broker.list\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"request.required.acks\": 1\n" +
                "}";

        ObjectMapper jsonMapper = new DefaultObjectMapper();
        jsonMapper.registerSubtypes(new NamedType(KafkaSink.class, "kafka"));
        KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        sink.open();
        Iterator<Message> msgIterator = new MessageSetReader(createMessageSet(TOPIC_NAME, 2)).iterator();
        while (msgIterator.hasNext()) {
            sink.writeTo(new StringMessage(msgIterator.next()));
        }
        assertTrue(sink.getNumOfPendingMessages() > 0);
        sink.close();
        assertEquals(sink.getNumOfPendingMessages(), 0);
        System.out.println(sink.getStat());

        // get the leader
        Option<Object> leaderOpt = ZkUtils.getLeaderForPartition(zk.getZkClient(), TOPIC_NAME, 0);
        assertTrue("Leader for topic new-topic partition 0 should exist", leaderOpt.isDefined());
        int leader = (Integer) leaderOpt.get();

        KafkaConfig config;
        if (leader == kafkaServer.getServer(0).config().brokerId()) {
            config = kafkaServer.getServer(0).config();
        } else {
            config = kafkaServer.getServer(1).config();
        }
        SimpleConsumer consumer = new SimpleConsumer(config.hostName(), config.port(), 100000, 100000, "clientId");
        FetchResponse response = consumer.fetch(new FetchRequestBuilder().addFetch(TOPIC_NAME, 0, 0, 100000).build());

        List<MessageAndOffset> messageSet = Lists.newArrayList(response.messageSet(TOPIC_NAME, 0).iterator());
        assertEquals("Should have fetched 2 messages", 2, messageSet.size());

        assertEquals(new String(extractMessage(messageSet, 0)), "testMessage" + 0);
        assertEquals(new String(extractMessage(messageSet, 1)), "testMessage" + 1);
    }

    @Test
    public void testMultithread() throws IOException {
        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", TOPIC_NAME_MULTITHREAD,
                        "--replication-factor", "2", "--partitions", "1"}));
        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"metadata.broker.list\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"request.required.acks\": 1,\n" +
                "    \"batchSize\": 10,\n" +
                "    \"jobQueueSize\": 3\n" +
                "}";

        ObjectMapper jsonMapper = new DefaultObjectMapper();
        jsonMapper.registerSubtypes(new NamedType(KafkaSink.class, "kafka"));
        KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        sink.open();
        int msgCount = 10000;
        for (int i = 0; i < msgCount; ++i) {
            Map<String, Object> msgMap = new ImmutableMap.Builder<String, Object>()
                    .put("key", Integer.toString(i))
                    .put("value", "message:" + i).build();
            sink.writeTo(new DefaultMessageContainer(
                    new Message(TOPIC_NAME_MULTITHREAD, jsonMapper.writeValueAsBytes(msgMap)),
                    jsonMapper));
        }
        assertTrue(sink.getNumOfPendingMessages() > 0);
        sink.close();
        System.out.println(sink.getStat());
        assertEquals(sink.getNumOfPendingMessages(), 0);

        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig("localhost:" + zk.getServerPort(), "gropuid_multhread"));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC_NAME_MULTITHREAD, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(TOPIC_NAME_MULTITHREAD).get(0);
        for (int i = 0; i < msgCount; ++i) {
            stream.iterator().next();
        }

        try {
            stream.iterator().next();
            fail();
        } catch (ConsumerTimeoutException e) {
            //this is expected
            consumer.shutdown();
        }
    }

    @Test
    public void testFileBasedQueuePartitionByKey() throws Exception {
        int numPartitions = 9;

        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", TOPIC_NAME_PARTITION_BY_KEY,
                        "--replication-factor", "2", "--partitions", Integer.toString(numPartitions)}));
        String fileQueue = String.format(
                "    \"queue4Sink\": {\n" +
                "        \"type\": \"file\",\n" +
                "        \"path\": \"%s\",\n" +
                "        \"name\": \"testKafkaSink\"\n" +
                "    }\n", tempDir.newFolder().getAbsolutePath());
        String keyTopicMap = String.format("   \"keyTopicMap\": {\n" +
                "        \"%s\": \"key\"\n" +
                "    }", TOPIC_NAME_PARTITION_BY_KEY);

        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"metadata.broker.list\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"request.required.acks\": 1,\n" +
                fileQueue + ",\n" +
                keyTopicMap + "\n" +
                "}";

        ObjectMapper jsonMapper = new DefaultObjectMapper();
        jsonMapper.registerSubtypes(new NamedType(KafkaSink.class, "kafka"));
        KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        sink.open();

        int messageCount = 10;
        for (int i = 0; i < messageCount; ++i) {
            Map<String, Object> msgMap = new ImmutableMap.Builder<String, Object>()
                    .put("key", Integer.toString(i % numPartitions))
                    .put("value", "message:" + i).build();
            sink.writeTo(new DefaultMessageContainer(
                    new Message(TOPIC_NAME_PARTITION_BY_KEY, jsonMapper.writeValueAsBytes(msgMap)),
                    jsonMapper));
        }
        sink.close();
        System.out.println(sink.getStat());

        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig("localhost:" + zk.getServerPort(), "gropuid"));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC_NAME_PARTITION_BY_KEY, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(TOPIC_NAME_PARTITION_BY_KEY).get(0);
        Map<Integer, Set<Map<String, Object>>> resultSet = new HashMap<Integer, Set<Map<String, Object>>>();
        for (int i = 0; i < messageCount; ++i) {
            MessageAndMetadata<byte[], byte[]> msgAndMeta = stream.iterator().next();
            System.out.println(new String(msgAndMeta.message()));

            Map<String, Object> msg = jsonMapper.readValue(new String(msgAndMeta.message()), new TypeReference<Map<String, Object>>() {});
            Set<Map<String, Object>> s = resultSet.get(msgAndMeta.partition());
            if (s == null) {
                s = new HashSet<Map<String, Object>>();
                resultSet.put(msgAndMeta.partition(), s);
            }
            s.add(msg);
        }

        int sizeSum = 0;
        for (Map.Entry<Integer, Set<Map<String, Object>>> e : resultSet.entrySet()) {
            sizeSum += e.getValue().size();
            String key = (String) e.getValue().iterator().next().get("key");
            for (Map<String, Object> ss : e.getValue()) {
                assertEquals(key, (String) ss.get("key"));
            }
        }
        assertEquals(sizeSum, messageCount);

        try {
            stream.iterator().next();
            fail();
        } catch (ConsumerTimeoutException e) {
            //this is expected
            consumer.shutdown();
        }
    }

    @Test
    public void testBlockingThreadPoolExecutor() {
        int jobQueueSize = 5;
        int corePoolSize = 3;
        int maxPoolSize = 3;

        try {
            testQueue(corePoolSize, maxPoolSize, new ArrayBlockingQueue<Runnable>(jobQueueSize));
            fail("RejectedExecutionException should be thrown");
        } catch (RejectedExecutionException e) {
            // good to go
        }

        BlockingQueue<Runnable> jobQueue = new ArrayBlockingQueue<Runnable>(jobQueueSize) {
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
        testQueue(corePoolSize, maxPoolSize, jobQueue);
    }

    private void testQueue(int corePoolSize, int maxPoolSize, BlockingQueue<Runnable> jobQueue) {
        ThreadPoolExecutor senders = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                10, TimeUnit.SECONDS,
                jobQueue);

        for (int i = 0; i < 100; ++i) {
            senders.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        fail();
                    }
                }
            });
        }
    }

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "20000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        props.put("consumer.timeout.ms", "3000");
        return new ConsumerConfig(props);
    }

    private byte[] extractMessage(List<MessageAndOffset> messageSet, int offset) {
        ByteBuffer bb = messageSet.get(offset).message().payload();
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes, 0, bytes.length);
        return bytes;
    }

    public static TMessageSet createMessageSet(String topic, int numMsgs) {
        MessageSetBuilder builder = new MessageSetBuilder(new ClientConfig()).withCompression(Compression.LZF);
        for (int i = 0; i < numMsgs; ++i) {
            builder.withMessage(topic, ("testMessage" + i).getBytes());
        }

        return builder.build();
    }
}
