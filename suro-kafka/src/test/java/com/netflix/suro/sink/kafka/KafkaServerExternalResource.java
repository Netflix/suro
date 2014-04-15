package com.netflix.suro.sink.kafka;

import com.google.common.collect.Lists;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.commons.lang.StringUtils;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class KafkaServerExternalResource extends ExternalResource {
    public static final int    BROKER_ID1 = 0;
    public static final int    BROKER_ID2 = 1;

    public static final int    KAFKA_PORT1 = 2200;
    public static final int    KAFKA_PORT2 = 2201;

    public static final long   TIMEOUT = 10000;

    private  KafkaConfig config1;
    private  KafkaConfig config2;

    private  KafkaServer server1;
    private  KafkaServer server2;

    private  List<KafkaServer> servers;
    private  List<KafkaConfig> configs;

    @Override
    protected void before() throws Throwable {
        config1 = new KafkaConfig(createBrokerConfig(BROKER_ID1, KAFKA_PORT1));
        server1 = createServer(config1);

        config2 = new KafkaConfig(createBrokerConfig(BROKER_ID2, KAFKA_PORT2));
        server2 = createServer(config2);

        servers = Lists.newArrayList(server1, server2);
        configs = Lists.newArrayList(config1, config2);
    }

    @Override
    protected void after() {
        if (server1 != null) {
            server1.shutdown();
            server1.awaitShutdown();
        }

        if (server2 != null) {
            server2.shutdown();
            server2.awaitShutdown();
        }
    }

    public String getBrokerListStr() {
        List<String> str = Lists.newArrayList();
        for (KafkaConfig config : configs) {
            str.add(config.hostName() + ":" + config.port());
        }
        return StringUtils.join(str, ",");
    }

    public KafkaServer getServer(int index) {
        return servers.get(index);
    }

    public static KafkaServer createServer(KafkaConfig config) {
        KafkaServer server = new KafkaServer(config, kafka.utils.SystemTime$.MODULE$);
        server.startup();
        return server;
    }

    public static File tempDir() {
        String ioDir = System.getProperty("java.io.tmpdir");
        File f = new File(ioDir, "kafka-" + new Random().nextInt(1000000));
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
        props.put("zookeeper.connect",           "localhost:" + ZkExternalResource.ZK_SERVER_PORT);
        props.put("replica.socket.timeout.ms",   "1500");
        props.put("hostName",                    "localhost");
        props.put("numPartitions",               "1");

        return props;
    }
}
