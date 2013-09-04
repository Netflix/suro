package com.netflix.suro.message;

import com.netflix.suro.message.serde.MessageSerDe;
import com.netflix.suro.message.serde.StringSerDe;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestMessageSerDe {
    @Test
    public void test() {
        MessageSerDe serde = new MessageSerDe();
        Message msg = new Message("routingKey",
                "app",
                "hostname",
                new StringSerDe(),
                "payload".getBytes());
        byte[] bytes = serde.serialize(msg);
        for (int i = 0; i < 100; ++i) {
            assertEquals(msg, serde.deserialize(bytes));
        }
    }
}
