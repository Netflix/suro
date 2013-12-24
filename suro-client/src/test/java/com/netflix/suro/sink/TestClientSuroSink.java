package com.netflix.suro.sink;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.suro.SuroPlugin;
import com.netflix.suro.SuroServer4Test;
import com.netflix.suro.connection.TestConnectionPool;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.StringMessage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestClientSuroSink {
    private List<SuroServer4Test> servers;

    @Before
    public void setup() throws Exception {
        servers = TestConnectionPool.startServers(3, 9200);
    }

    @After
    public void tearDown() {
        TestConnectionPool.shutdownServers(servers);
    }

    @Test
    public void test() throws IOException {
        String desc = "{\n" +
                "  \"type\":\"suro\",\n" +
                "  \"properties\": {\n" +
                "    \"SuroClient.loadBalancerServer\":\"localhost:9200,localhost:9201,localhost:9202\",\n" +
                "    \"SuroClient.loadBalancerType\":\"static\",\n" +
                "    \"SuroClient.clientType\":\"sync\"\n" +
                "  }\n" +
                "}";

        Injector injector = Guice.createInjector(
                new ClientSinkPlugin(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(ObjectMapper.class).to(DefaultObjectMapper.class);
                    }
                }
            );

        ObjectMapper jsonMapper = injector.getInstance(DefaultObjectMapper.class);
        Sink sink = jsonMapper.readValue(desc, new TypeReference<Sink>(){});
        sink.open();

        sink.writeTo(new StringMessage("routingKey", "testMessage"));
        assertEquals(sink.getStat(), "sent: 1" + "\n" + "lost: 0");

        TestConnectionPool.checkMessageCount(servers, 1);

        sink.close();
    }
}
