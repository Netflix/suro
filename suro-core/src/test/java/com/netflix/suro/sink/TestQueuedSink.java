package com.netflix.suro.sink;

import com.netflix.suro.message.Message;
import com.netflix.suro.queue.FileQueue4Sink;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestQueuedSink {
    @Rule
    public TemporaryFolder folder= new TemporaryFolder();

    @Test
    public void testDrainOnce() throws IOException {
        FileQueue4Sink queue = new FileQueue4Sink(folder.newFolder().getAbsolutePath(), "testDrainOnce", "PT1h");
        final List<Message> sentMessageList = new LinkedList<Message>();

        QueuedSink sink = new QueuedSink() {
            @Override
            protected void beforePolling() throws IOException {

            }

            @Override
            protected void write(List<Message> msgList) throws IOException {
                sentMessageList.addAll(msgList);
                msgList.clear();

            }

            @Override
            protected void innerClose() throws IOException {

            }
        };
        sink.initialize(queue, 100, 1000);
        sink.start();

        int msgCount = 1000;
        for (int i = 0; i < msgCount; ++i) {
            queue.offer(new Message("routingKey", ("message" + i).getBytes()));
        }

        sink.close();

        assertEquals(sentMessageList.size(), msgCount);

    }

}
