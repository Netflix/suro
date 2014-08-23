package com.netflix.suro.message;

import com.netflix.suro.sink.kafka.SuroKeyedMessage;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    @Test
    public void testMultiThreaded() throws InterruptedException {
        final MessageSerDe serde = new MessageSerDe();
        int threadCount = 10;
        ExecutorService executors = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final CountDownLatch startLatch = new CountDownLatch(1);

        for (int i = 0; i < threadCount; ++i) {
            executors.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    for (int j = 0; j < 10000; ++j) {
                        String str = generateRandomString();
                        Message msg = new Message("testRoutingKey", str.getBytes());
                        byte[] bytes = serde.serialize(msg);
                        Message bmsg = serde.deserialize(bytes);
                        assertEquals(bmsg.getRoutingKey(), "testRoutingKey");
                        assertEquals(new String(bmsg.getPayload()), str);
                    }

                    latch.countDown();
                }
            });
        }
        startLatch.countDown();
        latch.await();
    }

    public String generateRandomString() {
        Random rand = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4096; ++i) {
            sb.append((char) (rand.nextInt(95) + 32));
        }

        return sb.toString();
    }
}
