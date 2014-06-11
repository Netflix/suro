package com.netflix.suro.input;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.netflix.governator.configuration.PropertiesConfigurationProvider;
import com.netflix.governator.guice.BootstrapBinder;
import com.netflix.governator.guice.BootstrapModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.suro.TestUtils;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.sink.kafka.KafkaServerExternalResource;
import com.netflix.suro.sink.kafka.ZkExternalResource;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;

public class TestInputManager {
    public static ZkExternalResource zk = new ZkExternalResource();
    public static KafkaServerExternalResource kafkaServer = new KafkaServerExternalResource(zk);

    @ClassRule
    public static TestRule chain = RuleChain
            .outerRule(zk)
            .around(kafkaServer);

    private String inputConfig = "[\n" +
            "    {\n" +
            "        \"type\": \"thrift\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"kafka\",\n" +
            "        \"topic\": \"kafka_topic\",\n" +
            "        \"consumerProps\": {\n" +
            "            \"group.id\": \"kafka1\",\n" +
            "            \"zookeeper.connect\":\"" + zk.getConnectionString() + "\",\n" +
            "            \"consumer.timeout.ms\": \"1000\"\n" +
            "        }\n" +
            "    }\n" +
            "]";

    private String addInputConfig = "[\n" +
            "    {\n" +
            "        \"type\": \"thrift\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"kafka\",\n" +
            "        \"topic\": \"kafka_topic\",\n" +
            "        \"consumerProps\": {\n" +
            "            \"group.id\": \"kafka1\",\n" +
            "            \"zookeeper.connect\":\"" + zk.getConnectionString() + "\",\n" +
            "            \"consumer.timeout.ms\": \"1000\"\n" +
            "        }\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"kafka\",\n" +
            "        \"topic\": \"kafka_topic\",\n" +
            "        \"consumerProps\": {\n" +
            "            \"group.id\": \"kafka2\",\n" +
            "            \"zookeeper.connect\":\"" + zk.getConnectionString() + "\",\n" +
            "            \"consumer.timeout.ms\": \"1000\"\n" +
            "        }\n" +
            "    }\n" +
            "]";

    @Test
    public void test() throws Exception {
        int statusPort = TestUtils.pickPort();
        int serverPort = TestUtils.pickPort();

        final Properties props = new Properties();
        props.put("SuroServer.statusServerPort", Integer.toString(statusPort));
        props.put("SuroServer.port", Integer.toString(serverPort));

        Injector injector = LifecycleInjector.builder().withBootstrapModule(new BootstrapModule() {
            @Override
            public void configure(BootstrapBinder binder) {
                binder.bindConfigurationProvider().toInstance(new PropertiesConfigurationProvider(props));
            }
        }).withModules(
                new SuroInputPlugin(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(ObjectMapper.class).to(DefaultObjectMapper.class);
                    }
                }
        ).build().createInjector();

        LifecycleManager lifecycleManager = injector.getInstance(LifecycleManager.class);
        lifecycleManager.start();

        InputManager inputManager = new InputManager();
        List<SuroInput> inputList = injector.getInstance(ObjectMapper.class).readValue(
                inputConfig,
                new TypeReference<List<SuroInput>>() {
                });
        inputManager.set(inputList);
        assertNotNull(inputManager.getInput("thrift"));
        assertNotNull(inputManager.getInput("kafka_topic-kafka1"));

        inputList = injector.getInstance(ObjectMapper.class).readValue(
                addInputConfig,
                new TypeReference<List<SuroInput>>() {
                });
        inputManager.set(inputList);
        assertNotNull(inputManager.getInput("thrift"));
        assertNotNull(inputManager.getInput("kafka_topic-kafka1"));
        assertNotNull(inputManager.getInput("kafka_topic-kafka2"));


        inputList = injector.getInstance(ObjectMapper.class).readValue(
                inputConfig,
                new TypeReference<List<SuroInput>>() {
                });
        inputManager.set(inputList);
        assertNotNull(inputManager.getInput("thrift"));
        assertNotNull(inputManager.getInput("kafka_topic-kafka1"));
    }
}
