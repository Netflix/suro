package com.netflix.suro.sink.queue;

import com.netflix.suro.message.Message;
import com.netflix.suro.queue.FileQueue4Sink;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestFileQueue {
    @Rule
    public TemporaryFolder folder= new TemporaryFolder();

    @Test
    public void test() throws IOException {
        FileQueue4Sink queue = new FileQueue4Sink(folder.newFolder().getAbsolutePath(), "testqueue", "PT1m", 1024 * 1024 * 1024);
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
