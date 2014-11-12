package com.netflix.suro.sink.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
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
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.junit.BeforeClass;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
    private static final String TOPIC_NAME_BACKWARD_COMPAT = "routingKey_backwardCompat";

    private static ObjectMapper jsonMapper = new DefaultObjectMapper();

    @BeforeClass
    public static void startup() {
        jsonMapper.registerSubtypes(new NamedType(KafkaSink.class, "kafka"));
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
    }

    @Test
    public void testDefaultParameters() throws IOException {
        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", TOPIC_NAME,
                        "--replication-factor", "2", "--partitions", "1"}));
        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"acks\": 1\n" +
                "}";


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
                "    \"bootstrap.servers\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"acks\": 1\n" +
                "}";

        KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>() {
        });
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
    public void testPartitionByKey() throws Exception {
        int numPartitions = 9;

        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", TOPIC_NAME_PARTITION_BY_KEY,
                        "--replication-factor", "2", "--partitions", Integer.toString(numPartitions)}));
        String keyTopicMap = String.format("   \"keyTopicMap\": {\n" +
                "        \"%s\": \"key\"\n" +
                "    }", TOPIC_NAME_PARTITION_BY_KEY);

        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"acks\": 1,\n" +
                keyTopicMap + "\n" +
                "}";

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
    public void testCheckPause() throws IOException, InterruptedException {
        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", TOPIC_NAME + "check_pause",
                        "--replication-factor", "2", "--partitions", "1"}));
        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"acks\": 1,\n" +
                "    \"buffer.memory\": 1000,\n" +
                "    \"batch.size\": 1000\n" +
                "}";


        final KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        sink.open();
        boolean exceptionCaught = false;
        boolean checkPaused = false;
        boolean pending = false;

        for (int i = 0; i < 100; ++i) {
            try {
                sink.writeTo(new DefaultMessageContainer(new Message(TOPIC_NAME + "check_pause", getBigData()), jsonMapper));
            } catch (BufferExhaustedException e) {
                exceptionCaught = true;
                if (sink.checkPause() > 0) {
                    checkPaused = true;
                }
                if (sink.getNumOfPendingMessages() > 0) {
                    pending = true;
                }
            } catch (Exception e) {
                fail("invalid exception:" + e.toString());
            }
        }
        assertTrue(exceptionCaught);
        assertTrue(checkPaused);
        assertTrue(pending);
    }

    @Test
    public void testBlockingOnBufferFull() throws Throwable {
        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", TOPIC_NAME + "buffer_full",
                        "--replication-factor", "2", "--partitions", "1"}));
        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"acks\": 1,\n" +
                "    \"block.on.buffer.full\": true,\n" +
                "    \"buffer.memory\": 1000,\n" +
                "    \"batch.size\": 1000\n" +
                "}";


        final KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        sink.open();
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {

            @Override
            public void run() {
                for (int i = 0; i < 100; ++i) {
                    try {
                        sink.writeTo(new DefaultMessageContainer(new Message(TOPIC_NAME + "buffer_full", getBigData()), jsonMapper));
                    } catch (Exception e) {
                        fail("exception thrown: " + e.toString());
                    }
                    if (i == 50) {
                        kafkaServer.after(); // to simulate kafka latency
                    }
                }
                latch.countDown();
            }
        }).start();
        latch.await(3, TimeUnit.SECONDS);
        assertEquals(latch.getCount(), 1); // blocked

        kafkaServer.before();
    }

    @Test
    public void testConfigBackwardCompatible() throws IOException {
        int numPartitions = 9;

        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", TOPIC_NAME_BACKWARD_COMPAT,
                        "--replication-factor", "2", "--partitions", Integer.toString(numPartitions)}));
        String keyTopicMap = String.format("   \"keyTopicMap\": {\n" +
                "        \"%s\": \"key\"\n" +
                "    }", TOPIC_NAME_BACKWARD_COMPAT);

        String description1 = "{\n" +
                "    \"type\": \"Kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"ack\": 1,\n" +
                "     \"compression.type\": \"snappy\",\n" +
                keyTopicMap + "\n" +
                "}";
        String description2 = "{\n" +
                "    \"type\": \"Kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"metadata.broker.list\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"request.required.acks\": 1,\n" +
                "     \"compression.codec\": \"snappy\",\n" +
                keyTopicMap + "\n" +
                "}";

        // setup sinks, both old and new versions
        ObjectMapper jsonMapper = new DefaultObjectMapper();
        jsonMapper.registerSubtypes(new NamedType(KafkaSink.class, "Kafka"));
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
        KafkaSink sink1 = jsonMapper.readValue(description1, new TypeReference<Sink>(){});
        KafkaSink sink2 = jsonMapper.readValue(description2, new TypeReference<Sink>(){});
        sink1.open();
        sink2.open();
        List<Sink> sinks = new ArrayList<Sink>();
        sinks.add(sink1);
        sinks.add(sink2);

        // setup Kafka consumer (to read back messages)
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig("localhost:" + zk.getServerPort(), "gropuid"));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC_NAME_BACKWARD_COMPAT, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(TOPIC_NAME_BACKWARD_COMPAT).get(0);

        // Send 20 test message, using the old and new Kafka sinks.
        // Retrieve the messages and ensure that they are identical and sent to the same partition.
        Random rand = new Random();
        int messageCount = 20;
        for (int i = 0; i < messageCount; ++i) {
            Map<String, Object> msgMap = new ImmutableMap.Builder<String, Object>()
                    .put("key", new Long( rand.nextLong() ) )
                    .put("value", "message:" + i).build();

            // send message to both sinks
            for( Sink sink : sinks ){
                sink.writeTo(new DefaultMessageContainer(
                        new Message(TOPIC_NAME_BACKWARD_COMPAT, jsonMapper.writeValueAsBytes(msgMap)),
                        jsonMapper));
            }

            // read two copies of message back from Kafka and check that partitions and data match
            MessageAndMetadata<byte[], byte[]> msgAndMeta1 = stream.iterator().next();
            MessageAndMetadata<byte[], byte[]> msgAndMeta2 = stream.iterator().next();
            System.out.println( "iteration: "+i+" partition1: "+msgAndMeta1.partition() );
            System.out.println( "iteration: "+i+" partition2: "+msgAndMeta2.partition() );
            assertEquals(msgAndMeta1.partition(), msgAndMeta2.partition());
            String msg1Str = new String( msgAndMeta1.message() );
            String msg2Str = new String( msgAndMeta2.message() );
            System.out.println( "iteration: "+i+" message1: "+msg1Str );
            System.out.println( "iteration: "+i+" message2: "+msg2Str );
            assertEquals(msg1Str, msg2Str);
        }

        // close sinks
        sink1.close();
        sink2.close();
        // close consumer
        try {
            stream.iterator().next();
            fail(); // there should be no data left to consume
        } catch (ConsumerTimeoutException e) {
            //this is expected
            consumer.shutdown();
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

    public byte[] getBigData() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 900; ++i) {
            sb.append('a');
        }
        return sb.toString().getBytes();
    }
}
