package com.netflix.suro.sink.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.MonitorRegistry;
import com.netflix.servo.monitor.CompositeMonitor;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.Monitor;
import com.netflix.servo.tag.TagList;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.TagKey;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.Compression;
import com.netflix.suro.message.DefaultMessageContainer;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageSetBuilder;
import com.netflix.suro.message.MessageSetReader;
import com.netflix.suro.message.StringMessage;
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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

import rx.functions.Action3;
import scala.Option;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;

public class TestKafkaSink {
    public static TemporaryFolder tempDir = new TemporaryFolder();
    public static ZkExternalResource zk = new ZkExternalResource(tempDir);
    public static KafkaServerExternalResource kafkaServer = new KafkaServerExternalResource(zk);

    @ClassRule
    public static TestRule chain = RuleChain
            .outerRule(tempDir)
            .around(zk)
            .around(kafkaServer);

    @Rule
    public TestName testName = new TestName();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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

    /**
     * open will success because localhost is a resolvable hostname
     * note that there is no broker running at "localhost:1"
     */
    @Test
    public void testOpenWithDownBroker() throws Exception {
        String sinkstr = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"localhost:1\",\n" +
                "    \"kafka.etc\": {\n" +
                "          \"acks\": \"1\"\n" +
                "      }\n" +
                "}";
        KafkaSink sink = jsonMapper.readValue(sinkstr, new TypeReference<Sink>(){});
        sink.open();
        Assert.assertTrue(sink.isOpened());
        sink.close();
    }

    /**
     * open will fail because localhost is a resolvable hostname
     */
    @Test
    public void testOpenWithInvalidDnsName() throws Exception {
        final String bootstrapServers = "junk.foo.bar.com:7101";
        String sinkstr = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"" + bootstrapServers + "\",\n" +
                "    \"kafka.etc\": {\n" +
                "          \"acks\": \"1\"\n" +
                "      }\n" +
                "}";
        KafkaSink sink = jsonMapper.readValue(sinkstr, new TypeReference<Sink>(){});

        final String errMsg = "DNS resolution failed for url in bootstrap.servers: " + bootstrapServers;
        Throwable expectedCause = new org.apache.kafka.common.config.ConfigException(errMsg);
        thrown.expect(expectedCause.getClass());
        thrown.expectMessage(errMsg);

        sink.open();
    }

    /**
     * open will success because localhost is a resolvable hostname
     * note that there is no broker running at "localhost:1"
     */
    @Test
    public void testCloseLatency() throws Exception {
        String sinkstr = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"localhost:1\",\n" +
                "    \"kafka.etc\": {\n" +
                "          \"acks\": \"1\"\n" +
                "      }\n" +
                "}";
        KafkaSink sink = jsonMapper.readValue(sinkstr, new TypeReference<Sink>(){});
        sink.open();
        Assert.assertTrue(sink.isOpened());
        long start = System.currentTimeMillis();
        sink.close();
        long closeLatency = System.currentTimeMillis() - start;
        System.out.println("closeLatency = " + closeLatency);
        // close should take less than 500 ms
        assertTrue(closeLatency < 500);
    }

    @Test
    public void testDefaultParameters() throws IOException {
        final String topic = testName.getMethodName();
        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", topic,
                        "--replication-factor", "2", "--partitions", "1"}));
        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"kafka.etc\": {\n" +
                "          \"acks\": \"1\"\n" +
                "      }\n" +
                "}";


        KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        sink.open();
        Iterator<Message> msgIterator = new MessageSetReader(createMessageSet(topic, 2)).iterator();
        while (msgIterator.hasNext()) {
            sink.writeTo(new StringMessage(msgIterator.next()));
        }
        assertTrue(sink.getNumOfPendingMessages() > 0);
        sink.close();
        assertEquals(sink.getNumOfPendingMessages(), 0);
        System.out.println(sink.getStat());

        // get the leader
        Option<Object> leaderOpt = ZkUtils.getLeaderForPartition(zk.getZkClient(), topic, 0);
        assertTrue("Leader for topic new-topic partition 0 should exist", leaderOpt.isDefined());
        int leader = (Integer) leaderOpt.get();

        KafkaConfig config;
        if (leader == kafkaServer.getServer(0).config().brokerId()) {
            config = kafkaServer.getServer(0).config();
        } else {
            config = kafkaServer.getServer(1).config();
        }
        SimpleConsumer consumer = new SimpleConsumer(config.hostName(), config.port(), 100000, 100000, "clientId");
        FetchResponse response = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 100000).build());

        List<MessageAndOffset> messageSet = Lists.newArrayList(response.messageSet(topic, 0).iterator());
        assertEquals("Should have fetched 2 messages", 2, messageSet.size());

        assertEquals(new String(extractMessage(messageSet, 0)), "testMessage" + 0);
        assertEquals(new String(extractMessage(messageSet, 1)), "testMessage" + 1);
    }

    @Test
    public void testMultithread() throws IOException {
        final String topic = testName.getMethodName();
        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", topic,
                        "--replication-factor", "2", "--partitions", "1"}));
        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"kafka.etc\": {\n" +
                "          \"acks\": \"1\"\n" +
                "      }\n" +
                "}";

        KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>() {
        });
        sink.open();
        int msgCount = 1000;
        sendMessages(topic, sink, msgCount);
        assertTrue(sink.getNumOfPendingMessages() > 0);
        sink.close();
        System.out.println(sink.getStat());
        assertEquals(0, sink.getNumOfPendingMessages());

        checkConsumer(topic, msgCount);
    }

    @Test
    public void testPartitionByKey() throws Exception {
        final String topic = testName.getMethodName();
        int numPartitions = 9;

        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", topic,
                        "--replication-factor", "2", "--partitions", Integer.toString(numPartitions)}));
        String keyTopicMap = String.format("   \"keyTopicMap\": {\n" +
                "        \"%s\": \"key\"\n" +
                "    }", topic);

        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"kafka.etc\": {\n" +
                "          \"acks\": \"1\"\n" +
                "      },\n" +
                keyTopicMap + "\n" +
                "}";

        KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        sink.open();

        int messageCount = 20;
        for (int i = 0; i < messageCount; ++i) {
            Map<String, Object> msgMap = new ImmutableMap.Builder<String, Object>()
                    .put("key", Integer.toString(i % numPartitions))
                    .put("value", "message:" + i).build();
            sink.writeTo(new DefaultMessageContainer(
                    new Message(topic, jsonMapper.writeValueAsBytes(msgMap)),
                    jsonMapper));
        }
        sink.close();
        System.out.println(sink.getStat());

        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig("localhost:" + zk.getServerPort(), "gropuid"));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        // map from string key to set of partitions that messages contains this key
        int recvCount = 0;
        Map<String, Set<Integer>> resultMap = new HashMap<String, Set<Integer>>();
        for (int i = 0; i < messageCount; ++i) {
            MessageAndMetadata<byte[], byte[]> msgAndMeta = stream.iterator().next();
            recvCount++;
            System.out.println(String.format("partition: %d, msg: %s", msgAndMeta.partition(), new String(msgAndMeta.message())));
            Map<String, Object> msg = jsonMapper.readValue(new String(msgAndMeta.message()), new TypeReference<Map<String, Object>>() {});
            final String key = (String) msg.get("key");
            Set<Integer> partSet = resultMap.get(key);
            if (partSet == null) {
                partSet = new HashSet<Integer>();
                resultMap.put(key, partSet);
            }
            partSet.add(msgAndMeta.partition());
        }

        for (Map.Entry<String, Set<Integer>> e : resultMap.entrySet()) {
            final String errMsg = String.format("all msgs for the same key should go to the same partitions: key = %s, partCount = %d",
                    e.getKey(), e.getValue().size());
            assertEquals(errMsg, 1, e.getValue().size());
        }
        assertEquals(messageCount, recvCount);

        try {
            stream.iterator().next();
            fail();
        } catch (ConsumerTimeoutException e) {
            //this is expected
            consumer.shutdown();
        }
    }

    @Test
    public void testBlockingOnBufferFull() throws Throwable {
        final String topic = testName.getMethodName();
        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", topic,
                        "--replication-factor", "2", "--partitions", "1"}));
        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"block.on.metadata.queue.full\": true,\n" +
                "    \"kafka.etc\": {\n" +
                "          \"block.on.buffer.full\": true,\n" +
                "          \"buffer.memory\": 1000,\n" +
                "          \"batch.size\": 1000,\n" +
                "          \"acks\": \"1\"\n" +
                "      }\n" +
                "}";

        final KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        sink.open();
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        new Thread(new Runnable() {

            @Override
            public void run() {
                for (int i = 0; i < 100; ++i) {
                    try {
                        sink.writeTo(new DefaultMessageContainer(new Message(topic, getBigData()), jsonMapper));
                    } catch (Exception e) {
                        fail("exception thrown: " + e.toString());
                    }
                    if (i == 50) {
                        try{
                            kafkaServer.shutdown(); // to simulate kafka latency
                        }finally {
                            shutdownLatch.countDown();
                        }
                    }
                }
                latch.countDown();
            }
        }).start();
        latch.await(3, TimeUnit.SECONDS);
        assertEquals(1, latch.getCount()); // blocked

        // Make sure the kafka server is restarted only if shutdown is successful.
        shutdownLatch.await();
        kafkaServer.before();
    }

    @Test
    public void testStartWithKafkaOutage() throws Throwable {
        final String topic = testName.getMethodName();

        TopicCommand.createTopic(zk.getZkClient(),
            new TopicCommand.TopicCommandOptions(new String[]{
                "--zookeeper", "dummy", "--create", "--topic", topic,
                "--replication-factor", "2", "--partitions", "1"}));

        String[] brokerList = kafkaServer.getBrokerListStr().split(",");
        int port1 = Integer.parseInt(brokerList[0].split(":")[1]);
        int port2 = Integer.parseInt(brokerList[1].split(":")[1]);

        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"kafka.etc\": {\n" +
                "          \"acks\": \"1\"\n" +
                "      }\n" +
                "}";

        kafkaServer.shutdown();


        final KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        sink.open();

        final int msgCount = 1000;
        final CountDownLatch latch = new CountDownLatch(1);
        sink.setRecordCounterListener(new Action3<Long, Long, Long>() {

            @Override
            public void call(Long queued, Long sent, Long dropped) {
                if (sent == msgCount) {
                    latch.countDown();
                }
            }
        });

        sendMessages(topic, sink, msgCount);

        kafkaServer.startServer(port1, port2); // running up
        assertTrue(latch.await(10, TimeUnit.SECONDS));

        sendMessages(topic, sink, msgCount);
        sink.close();

        checkConsumer(topic, 2 * msgCount);
    }

    @Test
    public void testRunningKafkaOutage() throws IOException, InterruptedException {
        final String topicName1 = testName.getMethodName() + "kafkaoutage2";
        final String topicName2 = testName.getMethodName() + "kafkaoutage3";

        TopicCommand.createTopic(zk.getZkClient(),
            new TopicCommand.TopicCommandOptions(new String[]{
                "--zookeeper", "dummy", "--create", "--topic", topicName1,
                "--replication-factor", "2", "--partitions", "1"}));
        TopicCommand.createTopic(zk.getZkClient(),
            new TopicCommand.TopicCommandOptions(new String[]{
                "--zookeeper", "dummy", "--create", "--topic", topicName2,
                "--replication-factor", "2", "--partitions", "1"}));

        String[] brokerList = kafkaServer.getBrokerListStr().split(",");
        int port1 = Integer.parseInt(brokerList[0].split(":")[1]);
        int port2 = Integer.parseInt(brokerList[1].split(":")[1]);

        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"kafka.etc\": {\n" +
                "          \"acks\": \"1\"\n" +
                "      }\n" +
                "}";

        final KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        sink.open();

        final CountDownLatch latch = new CountDownLatch(1);
        final int msgCount = 1000;
        sink.setRecordCounterListener(new Action3<Long, Long, Long>() {
            @Override
            public void call(Long queued, Long sent, Long dropped) {
                if (sent == msgCount) {
                    latch.countDown();
                }
            }
        });

        sendMessages(topicName1, sink, msgCount);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        checkConsumer(topicName1, msgCount);

        kafkaServer.shutdown();

        sendMessages(topicName2, sink, msgCount);

        kafkaServer.startServer(port1, port2);

        final CountDownLatch latch2 = new CountDownLatch(1);
        final AtomicInteger numSent = new AtomicInteger();
        sink.setRecordCounterListener(new Action3<Long, Long, Long>() {
            @Override
            public void call(Long queued, Long sent, Long dropped) {
                if (sent + dropped == 3 * msgCount) {
                    numSent.set((int) (2 * msgCount - dropped));
                    latch2.countDown();
                }
            }
        });

        sendMessages(topicName2, sink, msgCount);
        sink.close();

        assertTrue(latch2.await(10, TimeUnit.SECONDS));
        assertTrue(numSent.get() > 0);
        checkConsumer(topicName2, numSent.get());
    }

    @Test
    public void testNormailizeRoutingKey() throws Exception {
        // normalize topic name to lower case
        final String topic = testName.getMethodName().toLowerCase();
        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", topic,
                        "--replication-factor", "2", "--partitions", "1"}));
        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"normalizeRoutingKey\": true,\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"kafka.etc\": {\n" +
                "          \"acks\": \"1\"\n" +
                "      }\n" +
                "}";

        Option<Object> leaderOpt = null;
        for(int i = 0; i < 100; ++i) {
            // get the leader
            leaderOpt = ZkUtils.getLeaderForPartition(zk.getZkClient(), topic, 0);
            if(leaderOpt.isDefined()) {
                break;
            }
            Thread.sleep(10);
        }
        assertTrue("Leader for topic new-topic partition 0 should exist", leaderOpt.isDefined());
        final int leader = (Integer) leaderOpt.get();
        Thread.sleep(100);

        KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        sink.open();
        // change topic name to all upper case
        Iterator<Message> msgIterator = new MessageSetReader(createMessageSet(topic.toUpperCase(), 2)).iterator();
        while (msgIterator.hasNext()) {
            sink.writeTo(new StringMessage(msgIterator.next()));
        }
        assertTrue(sink.getNumOfPendingMessages() > 0);
        sink.close();
        assertEquals(sink.getNumOfPendingMessages(), 0);
        System.out.println(sink.getStat());

        KafkaConfig config;
        if (leader == kafkaServer.getServer(0).config().brokerId()) {
            config = kafkaServer.getServer(0).config();
        } else {
            config = kafkaServer.getServer(1).config();
        }
        SimpleConsumer consumer = new SimpleConsumer(config.hostName(), config.port(), 100000, 100000, "clientId");
        FetchResponse response = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 100000).build());

        List<MessageAndOffset> messageSet = Lists.newArrayList(response.messageSet(topic, 0).iterator());
        assertEquals("Should have fetched 2 messages", 2, messageSet.size());

        assertEquals(new String(extractMessage(messageSet, 0)), "testMessage" + 0);
        assertEquals(new String(extractMessage(messageSet, 1)), "testMessage" + 1);
    }

    @Test
    public void testStickyPartitioner() throws Exception {
        final String topic = testName.getMethodName();
        int numPartitions = 9;

        TopicCommand.createTopic(zk.getZkClient(),
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", topic,
                        "--replication-factor", "2", "--partitions", Integer.toString(numPartitions)}));
        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"partitioner\": \"sticky\",\n" +
                "    \"partitioner.sticky.interval\": \"1234\",\n" +
                "    \"kafka.etc\": {\n" +
                "          \"acks\": \"1\"\n" +
                "      }\n" +
                "}";

        KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        assertThat(sink.getPartitioner(), instanceOf(StickyPartitioner.class));
        StickyPartitioner stickyPartitioner = (StickyPartitioner) sink.getPartitioner();
        assertEquals(1234, stickyPartitioner.getInterval());
        sink.open();

        int messageCount = 50;
        for (int i = 0; i < messageCount; ++i) {
            Map<String, Object> msgMap = new ImmutableMap.Builder<String, Object>()
                    .put("key", Integer.toString(i % numPartitions))
                    .put("value", "message:" + i).build();
            sink.writeTo(new DefaultMessageContainer(
                    new Message(topic, jsonMapper.writeValueAsBytes(msgMap)),
                    jsonMapper));
        }
        sink.close();
        System.out.println(sink.getStat());

        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig("localhost:" + zk.getServerPort(), "gropuid"));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        // map from string key to set of partitions that messages contains this key
        int recvCount = 0;
        Map<String, Set<Integer>> resultMap = new HashMap<String, Set<Integer>>();
        for (int i = 0; i < messageCount; ++i) {
            MessageAndMetadata<byte[], byte[]> msgAndMeta = stream.iterator().next();
            recvCount++;
        }
        assertEquals(messageCount, recvCount);

        try {
            stream.iterator().next();
            fail();
        } catch (ConsumerTimeoutException e) {
            //this is expected
            consumer.shutdown();
        }
    }
    
    @Test
    public void testDropMessage()
    {
    	String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"bootstrap.servers\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
                "    \"kafka.etc\": {\n" +
                "          \"acks\": \"1\"\n" +
                "      }\n" +
                "}";

        try {
	        KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>() { 
	        });
	        sink.open();
	        sink.dropMessage("routingKey", "reason", null, null);
	        sink.dropMessage("routingKey", "reason", new RuntimeException(), null);
	        sink.dropMessage("routingKey", "reason", new RuntimeException(new IllegalStateException()), null);
	        
	        boolean foundNoException = false;
	        boolean foundException = false;
	        boolean foundCausedException = false;
	        
	        MonitorRegistry registry = DefaultMonitorRegistry.getInstance();
	        Collection<Monitor<?>> monitors = registry.getRegisteredMonitors();
	        for (Monitor<?> monitor : monitors) {
	        	System.out.println(monitor);
	            if(monitor instanceof CompositeMonitor<?>)
	            {
	            	CompositeMonitor<?> cm = (CompositeMonitor<?>) monitor;
	            	List<Monitor<?>> subMonitors = cm.getMonitors();
	            	for (Monitor<?> monitor2 : subMonitors) {
	            		TagList tl = monitor2.getConfig().getTags();
	            		String exception = tl.getValue("exceptionClass");
	            		String causedBy = tl.getValue("causedClass");
	            		String reason = tl.getValue("droppedReason");
	            		
	            		
	            		if("reason".equals(reason) && exception==null && causedBy==null)
	            			foundNoException=true;
	            		
	            		if("reason".equals(reason) && "RuntimeException".equals(exception) && causedBy==null)
	            			foundException=true;
	            	
	            		if("reason".equals(reason) && "RuntimeException".equals(exception) && "IllegalStateException".equals(causedBy))
	            			foundCausedException=true;
                    }
	            }
            }

	        assertTrue(foundNoException);
	        assertTrue(foundException);
	        assertTrue(foundCausedException);
	        
        } catch (IOException e) {
	        fail(e.getMessage());
        }
    }

    private void sendMessages(String topicName, KafkaSink sink, int msgCount) throws JsonProcessingException {
        for (int i = 0; i < msgCount; ++i) {
            Map<String, Object> msgMap = new ImmutableMap.Builder<String, Object>()
                .put("key", Integer.toString(i))
                .put("value", "message:" + i).build();
            sink.writeTo(new DefaultMessageContainer(
                new Message(topicName, jsonMapper.writeValueAsBytes(msgMap)),
                jsonMapper));
        }
    }

    private void checkConsumer(String topicName, int msgCount) throws IOException {
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
            createConsumerConfig("localhost:" + zk.getServerPort(), "groupid"));
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topicName, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topicName).get(0);
        for (int i = 0; i < msgCount; ++i) {
            try {
                stream.iterator().next();
            } catch (ConsumerTimeoutException e) {
                fail(String.format("%d messages are consumed among %d", i, msgCount));
            }
        }

        try {
            stream.iterator().next();
            fail();
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
