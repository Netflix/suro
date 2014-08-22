package com.netflix.suro.queue;

import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageSerDe;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@Ignore
public class FileQueueLimitTest {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();
    private static FileBlockingQueue<Message> bigQueue;

    @Test
    public void test1GB() throws InterruptedException, IOException {
        createQueue();

        final int msgSize = 4 * 1024; // 4kb
        final long msgCount = 1L * 1024L * 1024L * 1024L / msgSize;

        int consumed = 0;
        for (int i = 0; i < msgCount; ++i) {
            bigQueue.put(new Message("routingKey", randomString(msgSize).getBytes()));
            if (msgCount % 100000 == 0) {
                Message msg = bigQueue.poll();
                assertEquals(msg.getRoutingKey(), "routingKey");
                assertEquals(msg.getPayload().length, msgSize);
                ++consumed;
            }
        }

        while (!bigQueue.isEmpty()) {
            Message msg = bigQueue.poll();
            assertEquals(msg.getRoutingKey(), "routingKey");
            assertEquals(msg.getPayload().length, msgSize);
            ++consumed;
        }

        assertEquals(consumed, msgCount);
    }

    private void createQueue() throws IOException {
        String path = tempDir.newFolder().getAbsolutePath();
        System.out.println("path:" + path);
        bigQueue = new FileBlockingQueue<Message>(path, "load_test", 1, new MessageSerDe(), true);
    }

    static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static Random rnd = new Random();

    public static String randomString(int len )
    {
        StringBuilder sb = new StringBuilder( len );
        for( int i = 0; i < len; i++ )
            sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
        return sb.toString();
    }
}
