package com.netflix.suro.sink.kafka;

import com.google.common.collect.Lists;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.commons.lang.StringUtils;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Properties;

public class KafkaServerExternalResource extends ExternalResource {
    public static final int    BROKER_ID1 = 0;
    public static final int    BROKER_ID2 = 1;

    //public static final int    KAFKA_PORT1 = 2200;
    //public static final int    KAFKA_PORT2 = 2201;

    public static final long   TIMEOUT = 10000;

    private  KafkaConfig config1;
    private  KafkaConfig config2;

    private  KafkaServer server1;
    private  KafkaServer server2;

    private  List<KafkaServer> servers;
    private  List<KafkaConfig> configs;

    private final ZkExternalResource zk;
    private final TemporaryFolder tempDir = new TemporaryFolder();

    public KafkaServerExternalResource(ZkExternalResource zk) {
        this.zk = zk;
    }

    private static int getUnusedPort() throws IOException {
        ServerSocket ss = new ServerSocket(0);
        ss.setReuseAddress(false);
        int unusedPort = ss.getLocalPort();
        ss.close();
        return unusedPort;
    }

    @Override
    protected void before() throws Throwable {
        startServer(getUnusedPort(), getUnusedPort());
    }

    public void startServer(int port1, int port2) throws IOException {
        tempDir.create();

        config1 = new KafkaConfig(
            createBrokerConfig(BROKER_ID1, port1, zk.getServerPort(), tempDir.newFolder().getAbsolutePath()));
        server1 = createServer(config1);

        config2 = new KafkaConfig(
            createBrokerConfig(BROKER_ID2, port2, zk.getServerPort(), tempDir.newFolder().getAbsolutePath()));
        server2 = createServer(config2);

        servers = Lists.newArrayList(server1, server2);
        configs = Lists.newArrayList(config1, config2);
    }

    @Override
    protected void after() {
        shutdown();
    }

    public void shutdown() {
        if (server1 != null) {
            server1.shutdown();
            server1.awaitShutdown();
        }

        if (server2 != null) {
            server2.shutdown();
            server2.awaitShutdown();
        }

        tempDir.delete();
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

    public static Properties createBrokerConfig(int nodeId, int port, int zkPort, String dir) {
        Properties props = new Properties();
        props.put("broker.id",                   Integer.toString(nodeId));
        props.put("brokerId",                    Integer.toString(nodeId));
        props.put("host.name",                   "localhost");
        props.put("port",                        Integer.toString(port));
        props.put("log.dir", dir);
        props.put("log.flush.interval.messages", "1");
        props.put("zookeeper.connect",           "localhost:" + zkPort);
        props.put("replica.socket.timeout.ms",   "1500");
        props.put("hostName",                    "localhost");
        props.put("numPartitions",               "1");

        return props;
    }
}
