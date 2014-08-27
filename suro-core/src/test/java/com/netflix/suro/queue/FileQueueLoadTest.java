package com.netflix.suro.queue;

import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageSerDe;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@Ignore
public class FileQueueLoadTest {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();
    private static FileBlockingQueue<Message> bigQueue;

    // configurable parameters
    //////////////////////////////////////////////////////////////////
    private static int loop = 5;
    private static int totalItemCount = 100000;
    private static int producerNum = 4;
    private static int consumerNum = 1;
    private static int messageLength = 1024;
    //////////////////////////////////////////////////////////////////

    private static enum Status {
        ERROR,
        SUCCESS
    }

    private static class Result {
        Status status;
    }

    private static final AtomicInteger producingItemCount = new AtomicInteger(0);
    private static final AtomicInteger consumingItemCount = new AtomicInteger(0);
    private static final Set<String> itemSet = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());

    static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static Random rnd = new Random();

    public static String randomString(int len )
    {
        StringBuilder sb = new StringBuilder( len );
        for( int i = 0; i < len; i++ )
            sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
        return sb.toString();
    }

    private static class Producer extends Thread {
        private final CountDownLatch latch;
        private final Queue<Result> resultQueue;

        public Producer(CountDownLatch latch, Queue<Result> resultQueue) {
            this.latch = latch;
            this.resultQueue = resultQueue;
        }

        public void run() {
            Result result = new Result();
            String rndString = randomString(messageLength);
            try {
                latch.countDown();
                latch.await();

                while(true) {
                    int count = producingItemCount.incrementAndGet();
                    if(count > totalItemCount) break;
                    String item = rndString + count;
                    itemSet.add(item);
                    bigQueue.put(new Message("routing", item.getBytes()));
                }
                result.status = Status.SUCCESS;
            } catch (Exception e) {
                e.printStackTrace();
                result.status = Status.ERROR;
            }
            resultQueue.offer(result);
        }
    }

    private static class Consumer extends Thread {
        private final CountDownLatch latch;
        private final Queue<Result> resultQueue;

        public Consumer(CountDownLatch latch, Queue<Result> resultQueue) {
            this.latch = latch;
            this.resultQueue = resultQueue;
        }

        public void run() {
            Result result = new Result();
            try {
                latch.countDown();
                latch.await();

                while(true) {
                    int index = consumingItemCount.getAndIncrement();
                    if (index >= totalItemCount) break;

                    Message item = bigQueue.take();
                    assertNotNull(item);
                    assertTrue(itemSet.remove(new String(item.getPayload())));
                }
                result.status = Status.SUCCESS;
            } catch (Exception e) {
                e.printStackTrace();
                result.status = Status.ERROR;
            }
            resultQueue.offer(result);
        }

    }

    @Test
    public void runTest() throws Exception {
        createQueue();

        System.out.println("Load test begin ...");
        for(int i = 0; i < loop; i++) {
            System.out.println("[doRunProduceThenConsume] round " + (i + 1) + " of " + loop);
            this.doRunProduceThenConsume();

            // reset
            producingItemCount.set(0);
            consumingItemCount.set(0);
        }

        bigQueue.close();
        createQueue();

        for(int i = 0; i < loop; i++) {
            System.out.println("[doRunMixed] round " + (i + 1) + " of " + loop);
            this.doRunMixed();

            // reset
            producingItemCount.set(0);
            consumingItemCount.set(0);
        }
        System.out.println("Load test finished successfully.");
    }

    private void createQueue() throws IOException {
        bigQueue = new FileBlockingQueue<Message>(tempDir.newFolder().getAbsolutePath(), "load_test", 1, new MessageSerDe());
    }

    public void doRunProduceThenConsume() throws Exception {
        //prepare
        CountDownLatch platch = new CountDownLatch(producerNum);
        CountDownLatch clatch = new CountDownLatch(consumerNum);
        BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
        BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<Result>();

        //run testing
        for(int i = 0; i < producerNum; i++) {
            Producer p = new Producer(platch, producerResults);
            p.start();
        }

        for(int i = 0; i < producerNum; i++) {
            Result result = producerResults.take();
            assertEquals(result.status, Status.SUCCESS);
        }

        assertTrue(!bigQueue.isEmpty());
        assertEquals(bigQueue.size(), totalItemCount);

        assertEquals(itemSet.size(), totalItemCount);

        for(int i = 0; i < consumerNum; i++) {
            Consumer c = new Consumer(clatch, consumerResults);
            c.start();
        }

        for(int i = 0; i < consumerNum; i++) {
            Result result = consumerResults.take();
            assertEquals(result.status, Status.SUCCESS);
        }

        assertTrue(itemSet.isEmpty());
        assertTrue(bigQueue.isEmpty());
        assertTrue(bigQueue.size() == 0);
    }


    public void doRunMixed() throws Exception {
        //prepare
        CountDownLatch allLatch = new CountDownLatch(producerNum + consumerNum);
        BlockingQueue<Result> producerResults = new LinkedBlockingQueue<Result>();
        BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<Result>();

        //run testing
        for(int i = 0; i < producerNum; i++) {
            Producer p = new Producer(allLatch, producerResults);
            p.start();
        }

        for(int i = 0; i < consumerNum; i++) {
            Consumer c = new Consumer(allLatch, consumerResults);
            c.start();
        }

        //verify
        for(int i = 0; i < producerNum; i++) {
            Result result = producerResults.take();
            assertEquals(result.status, Status.SUCCESS);
        }

        for(int i = 0; i < consumerNum; i++) {
            Result result = consumerResults.take();
            assertEquals(result.status, Status.SUCCESS);
        }

        assertTrue(itemSet.isEmpty());
        assertTrue(bigQueue.isEmpty());
        assertTrue(bigQueue.size() == 0);
    }

}