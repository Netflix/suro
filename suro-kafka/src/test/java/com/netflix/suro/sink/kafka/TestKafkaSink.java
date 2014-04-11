package com.netflix.suro.sink.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.Lists;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.*;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.thrift.TMessageSet;
import kafka.admin.TopicCommand;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestKafkaSink {
    private static final Logger LOG = LoggerFactory.getLogger(TestKafkaSink.class);

    private static final String ZK_SERVER_NAME  = TestKafkaSink.class.getSimpleName();
    private static final int    ZK_SERVER_PORT  = 2181;

    private static final String TOPIC_NAME = "routingKey";

    private static final int    BROKER_ID1 = 0;
    private static final int    BROKER_ID2 = 1;

    private static final int    KAFKA_PORT1 = 2200;
    private static final int    KAFKA_PORT2 = 2201;

    private static ZkServer zkServer;
    private static ZkClient zkClient;

    private static KafkaConfig config1;
    private static KafkaConfig config2;

    private static KafkaServer server1;
    private static KafkaServer server2;

    private static SimpleConsumer consumer1;
    private static SimpleConsumer consumer2;

    @BeforeClass
    public static void setup() throws Exception {
        zkServer = startZkServer();
        zkClient = new ZkClient("localhost:2181", 20000, 20000, new ZkSerializer() {
            @Override
            public byte[] serialize(Object data) throws ZkMarshallingError {
                try {
                    return ((String)data).getBytes("UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Object deserialize(byte[] bytes) throws ZkMarshallingError {
                if (bytes == null)
                    return null;
                try {
                    return new String(bytes, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        config1 = new KafkaConfig(createBrokerConfig(BROKER_ID1, KAFKA_PORT1));
        server1 = createServer(config1);

        config2 = new KafkaConfig(createBrokerConfig(BROKER_ID2, KAFKA_PORT2));
        server2 = createServer(config2);

        consumer1 = new SimpleConsumer("localhost", KAFKA_PORT1, 1000000, 64*1024, "");
        consumer2 = new SimpleConsumer("localhost", KAFKA_PORT2, 1000000, 64*1024, "");
    }

    @AfterClass
    public static void shutdown() throws Exception {
        if (server1 != null) {
            server1.shutdown();
            server1.awaitShutdown();
        }

        if (server2 != null) {
            server2.shutdown();
            server2.awaitShutdown();
        }

        if (zkServer != null) {
            zkServer.shutdown();
        }
    }

    public String getBrokerListStr() {
        List<String> str = Lists.newArrayList();
        str.add(config1.hostName() + ":" + config1.port());
        str.add(config2.hostName() + ":" + config2.port());
        return StringUtils.join(str, ",");
    }

    public static KafkaServer createServer(KafkaConfig config) {
        KafkaServer server = new KafkaServer(config, kafka.utils.SystemTime$.MODULE$);
        server.startup();
        return server;
    }

    public static File tempDir() {
        File f = new File("./build/test", "kafka-" + new Random().nextInt(1000000));
        f.mkdirs();
        f.deleteOnExit();
        return f;
    }

    public static Properties createBrokerConfig(int nodeId, int port) {
        Properties props = new Properties();
        props.put("broker.id",                   Integer.toString(nodeId));
        props.put("brokerId",                    Integer.toString(nodeId));
        props.put("host.name",                   "localhost");
        props.put("port",                        Integer.toString(port));
        props.put("log.dir",                     tempDir().getAbsolutePath());
        props.put("log.flush.interval.messages", "1");
        props.put("zookeeper.connect",           "localhost:" + ZK_SERVER_PORT);
        props.put("replica.socket.timeout.ms",   "1500");
        props.put("hostName",                    "localhost");
        props.put("numPartitions",               "1");

        System.out.println(props.toString());

        return props;
    }

    public static ZkServer startZkServer() throws Exception {
        String dataPath = "./build/test/" + ZK_SERVER_NAME + "/data";
        String logPath  = "./build/test/" + ZK_SERVER_NAME + "/log";
        FileUtils.deleteDirectory(new File(dataPath));
        FileUtils.deleteDirectory(new File(logPath));

        ZkServer zkServer = new ZkServer(
                dataPath,
                logPath,
                new IDefaultNameSpace() {
                    @Override
                    public void createDefaultNameSpace(ZkClient zkClient) {
                    }
                },
                ZK_SERVER_PORT,
                ZkServer.DEFAULT_TICK_TIME, 100);
        zkServer.start();
        return zkServer;
    }

    @Test
    public void test() throws IOException {
        TopicCommand.createTopic(zkClient,
                new TopicCommand.TopicCommandOptions(new String[]{
                        "--zookeeper", "dummy", "--create", "--topic", TOPIC_NAME,
                        "--replication-factor", "2", "--partitions", "1"}));
        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"metadata.broker.list\": \"" + getBrokerListStr() + "\",\n" +
                "    \"request.required.acks\": 1\n" +
                "}";

        ObjectMapper jsonMapper = new DefaultObjectMapper();
        jsonMapper.registerSubtypes(new NamedType(KafkaSink.class, "kafka"));
        KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
        sink.open();
        Iterator<Message> msgIterator = new MessageSetReader(createMessageSet(2)).iterator();
        while (msgIterator.hasNext()) {
            sink.writeTo(new StringMessage(msgIterator.next()));
        }
        sink.close();
        System.out.println(sink.getStat());

        // get the leader
        Option<Object> leaderOpt = ZkUtils.getLeaderForPartition(zkClient, TOPIC_NAME, 0);
        assertTrue("Leader for topic new-topic partition 0 should exist", leaderOpt.isDefined());
        int leader = (Integer) leaderOpt.get();

        FetchResponse response;
        if (leader == server1.config().brokerId()) {
            response = consumer1.fetch(new FetchRequestBuilder().addFetch(TOPIC_NAME, 0, 0, 100000).build());
        } else {
            response = consumer2.fetch(new FetchRequestBuilder().addFetch(TOPIC_NAME, 0, 0, 100000).build());
        }
        List<MessageAndOffset> messageSet = Lists.newArrayList(response.messageSet(TOPIC_NAME, 0).iterator());
        assertEquals("Should have fetched 2 messages", 2, messageSet.size());

        assertEquals(new String(extractMessage(messageSet, 0)), "testMessage" + 0);
        assertEquals(new String(extractMessage(messageSet, 1)), "testMessage" + 1);
    }

    private byte[] extractMessage(List<MessageAndOffset> messageSet, int offset) {
        ByteBuffer bb = messageSet.get(offset).message().payload();
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes, 0, bytes.length);
        return bytes;
    }

    public static TMessageSet createMessageSet(int numMsgs) {
        MessageSetBuilder builder = new MessageSetBuilder(new ClientConfig()).withCompression(Compression.LZF);
        for (int i = 0; i < numMsgs; ++i) {
            builder.withMessage(TOPIC_NAME, ("testMessage" + i).getBytes());
        }

        return builder.build();
    }
}
