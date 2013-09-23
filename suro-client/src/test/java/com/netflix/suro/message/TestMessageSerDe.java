package com.netflix.suro.message;

import com.netflix.suro.message.serde.MessageSerDe;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestMessageSerDe {
    @Test
    public void test() {
        MessageSerDe serde = new MessageSerDe();
        for (int k = 0; k < 10; ++k) {
            Message msg = new Message("routingKey",
                    "payload".getBytes());
            byte[] bytes = serde.serialize(msg);
            for (int i = 0; i < 100; ++i) {
                assertEquals(msg, serde.deserialize(bytes));
            }
        }
    }
}
