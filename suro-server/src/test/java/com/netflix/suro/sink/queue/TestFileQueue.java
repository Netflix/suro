package com.netflix.suro.sink.queue;

import com.leansoft.bigqueue.utils.FileUtil;
import com.netflix.suro.message.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestFileQueue {
    private String dir;

    @After
    @Before
    public void setup() {
        dir = System.getProperty("java.io.tmpdir") + "/testqueue";
        FileUtil.deleteDirectory(new File(dir));
    }
    @Test
    public void test() throws IOException {
        FileQueue4Sink queue = new FileQueue4Sink(dir, "testqueue", "PT1m");
        assertEquals(queue.size(), 0);
        assertEquals(queue.isEmpty(), true);
        assertEquals(queue.retrieve(100, new LinkedList<Message>()), 0);

        for (int i = 0; i < 100; ++i) {
            queue.put(new Message("routingkey" + i, ("value" + i).getBytes()));
        }

        assertEquals(queue.size(), 100);
        assertEquals(queue.isEmpty(), false);

        List<Message> msgList = new LinkedList<Message>();
        assertEquals(queue.retrieve(100, msgList), 100);
        int i = 0;
        for (Message m : msgList) {
            assertEquals(m.getRoutingKey(), "routingkey" + i);
            assertEquals(new String(m.getPayload()), "value" + i);
            ++i;
        }
        msgList.clear();
        assertEquals(i, 100);

        assertEquals(queue.size(), 100);
        assertEquals(queue.isEmpty(), false);

        assertEquals(queue.retrieve(100, msgList), 100);
        i = 0;
        for (Message m : msgList) {
            assertEquals(m.getRoutingKey(), "routingkey" + i);
            assertEquals(new String(m.getPayload()), "value" + i);
            ++i;
        }
        assertEquals(i, 100);

        queue.commit();
        assertEquals(queue.size(), 0);
        assertEquals(queue.isEmpty(), true);
        assertEquals(queue.retrieve(100, msgList), 0);
    }

    @Test
    public void testGC() throws IOException, InterruptedException {
        FileQueue4Sink queue = new FileQueue4Sink(dir, "testqueue", "PT1s");
        assertEquals(queue.size(), 0);
        assertEquals(queue.isEmpty(), true);
        assertEquals(queue.retrieve(100, new LinkedList<Message>()), 0);

        for (int i = 0; i < 100; ++i) {
            queue.put(new Message("routingkey" + i, ("value" + i).getBytes()));
        }

        LinkedList<Message> msgList = new LinkedList<Message>();
        queue.retrieve(50, msgList);
        Thread.sleep(2000); // wait until gc kicks in
        assertEquals(queue.size(), 100);
        queue.commit();
        msgList.clear();
        Thread.sleep(2000);
        assertEquals(queue.size(), 50);

        queue.retrieve(50, msgList);
        int i = 50;
        for (Message m : msgList) {
            assertEquals(m.getRoutingKey(), "routingkey" + i);
            assertEquals(new String(m.getPayload()), "value" + i);
            ++i;
        }
        assertEquals(i, 100);
        queue.commit();
        queue.close();

        queue = new FileQueue4Sink(dir, "testqueue", "PT1s");
        assertEquals(queue.size(), 0);
        assertEquals(queue.isEmpty(), true);
    }
}
