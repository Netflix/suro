package com.netflix.suro.sink;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.suro.connection.TestConnectionPool;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageSetReader;
import com.netflix.suro.sink.kafka.KafkaSink;
import org.junit.Test;

import java.io.IOException;

public class TestKafkaSink {
    @Test
    public void test() throws IOException {
        String description = "{\n" +
                "    \"type\": \"kafka\",\n" +
                "    \"client.id\": \"kafkasink\",\n" +
                "    \"metadata.broker.list\": \"ec2-54-234-153-254.compute-1.amazonaws.com:9092\",\n" +
                "    \"request.required.acks\": 1\n" +
                "}";

        ObjectMapper jsonMapper = new DefaultObjectMapper();
        try {
            KafkaSink sink = jsonMapper.readValue(description, new TypeReference<Sink>(){});
            sink.open();
            for (Message m : new MessageSetReader(TestConnectionPool.createMessageSet(100000))) {
                sink.writeTo(m);
            }
            sink.close();
            System.out.println(sink.getStat());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }
}
