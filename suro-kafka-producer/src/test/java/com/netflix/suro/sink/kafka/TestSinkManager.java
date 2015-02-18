package com.netflix.suro.sink.kafka;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.netflix.suro.jackson.DefaultObjectMapper;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

public class TestSinkManager {
    private static ObjectMapper jsonMapper = new DefaultObjectMapper();

    public static TemporaryFolder tempFolder = new TemporaryFolder();
    public static ZkExternalResource zk = new ZkExternalResource(tempFolder);
    public static KafkaServerExternalResource kafkaServer = new KafkaServerExternalResource(zk);

    @ClassRule
    public static TestRule chain = RuleChain
            .outerRule(tempFolder)
            .around(zk)
            .around(kafkaServer);

    @Rule
    public TestName testName = new TestName();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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

//    @Test
//    public void testSinkOpenFailure() throws Exception {
//        // override the interval from default 60s to 1s
//        ConfigurationManager.getConfigInstance().setProperty("suro.SinkManager.sinkCheckInterval", "1");
//
//        String[] brokerList = kafkaServer.getBrokerListStr().split(",");
//        int port1 = Integer.parseInt(brokerList[0].split(":")[1]);
//        int port2 = Integer.parseInt(brokerList[1].split(":")[1]);
//
//        String sink1str = "{\n" +
//                "    \"type\": \"kafka\",\n" +
//                "    \"client.id\": \"kafkasink\",\n" +
//                "    \"bootstrap.servers\": \"" + kafkaServer.getBrokerListStr() + "\",\n" +
//                "    \"kafka.etc\": {\n" +
//                "          \"acks\": \"1\"\n" +
//                "      }\n" +
//                "}";
//
//        // create topic
//        final String topic = testName.getMethodName();
//        TopicCommand.createTopic(zk.getZkClient(),
//                new TopicCommand.TopicCommandOptions(new String[]{
//                        "--zookeeper", "dummy", "--create", "--topic", topic,
//                        "--replication-factor", "2", "--partitions", "1"}));
//
//        // shutdown kafka servers
//        kafkaServer.shutdown();
//
//        // create first sink
//        KafkaSink sink1 = jsonMapper.readValue(sink1str, new TypeReference<Sink>(){});
//
//        // create sink manager
//        SinkManager sinkManager = new SinkManager();
//        sinkManager.initialSet(ImmutableMap.<String, Sink>of("sink1", sink1));
//        sinkManager.initialStart();
//
//        Assert.assertFalse(sink1.isOpened());
//    }
}
