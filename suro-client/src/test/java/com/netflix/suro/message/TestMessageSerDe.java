package com.netflix.suro.message;

import com.netflix.suro.sink.kafka.SuroKeyedMessage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestMessageSerDe {
    @Test
    public void testMessage() {
        MessageSerDe serde = new MessageSerDe();
        for (int k = 0; k < 10; ++k) {
            Message msg = new Message("routingKey",
                    "payload".getBytes());
            byte[] bytes = serde.serialize(msg);
            assertEquals(msg, serde.deserialize(bytes));
        }
    }

    @Test
    public void testSuroKeyedMessage() {
        MessageSerDe serde = new MessageSerDe();
        for (int k = 0; k < 10; ++k) {
            Message msg = new SuroKeyedMessage(
                    k,
                    new Message("routingKey", "payload".getBytes()));
            byte[] bytes = serde.serialize(msg);
            SuroKeyedMessage suroKeyedMessage = (SuroKeyedMessage) serde.deserialize(bytes);
            assertEquals(msg, suroKeyedMessage);
            assertEquals(suroKeyedMessage.getKey(), k);
        }
    }
}
