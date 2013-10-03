package com.netflix.suro.sink.queue;

import com.netflix.suro.message.Message;
import com.netflix.suro.queue.MemoryQueue4Sink;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestMemoryQueue {
    @Test
    public void test() throws IOException {
        MemoryQueue4Sink queue = new MemoryQueue4Sink(100);
        assertEquals(queue.size(), 0);
        assertEquals(queue.isEmpty(), true);
        assertEquals(queue.drain(100, new LinkedList<Message>()), 0);

        for (int i = 0; i < 100; ++i) {
            queue.offer(new Message("routingkey" + i, ("value" + i).getBytes()));
        }

        assertEquals(queue.size(), 100);
        assertEquals(queue.isEmpty(), false);

        List<Message> msgList = new LinkedList<Message>();
        assertEquals(queue.drain(100, msgList), 100);
        int i = 0;
        for (Message m : msgList) {
            assertEquals(m.getRoutingKey(), "routingkey" + i);
            assertEquals(new String(m.getPayload()), "value" + i);
            ++i;
        }
        assertEquals(i, 100);

        assertEquals(queue.size(), 0);
        assertEquals(queue.isEmpty(), true);
    }
}
