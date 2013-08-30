package com.netflix.suro.sink.queue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.leansoft.bigqueue.BigArrayImpl;
import com.leansoft.bigqueue.IBigArray;
import com.leansoft.bigqueue.page.IMappedPage;
import com.leansoft.bigqueue.page.IMappedPageFactory;
import com.leansoft.bigqueue.page.MappedPageFactoryImpl;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.serde.MessageSerDe;
import com.netflix.suro.message.serde.SerDe;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@NotThreadSafe
public class FileQueue4Sink implements MessageQueue4Sink {
    static Logger log = LoggerFactory.getLogger(FileQueue4Sink.class);

    public static final String TYPE = "file";

    final IBigArray innerArray;
    // 2 ^ 3 = 8
    final static int QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS = 3;
    // size in bytes of queue front index page
    final static int QUEUE_FRONT_INDEX_PAGE_SIZE = 1 << QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS;
    // only use the first page
    static final long QUEUE_FRONT_PAGE_INDEX = 0;

    // folder name for queue front index page
    final static String QUEUE_FRONT_INDEX_PAGE_FOLDER = "front_index";

    // front index of the big queue,
    final AtomicLong queueFrontIndex = new AtomicLong();

    // factory for queue front index page management(acquire, release, cache)
    IMappedPageFactory queueFrontIndexPageFactory;

    // locks for queue front write management
    final Lock queueFrontWriteLock = new ReentrantLock();

    private SerDe<Message> serde = new MessageSerDe();
    private long consumedIndex;
    private Period gcPeriod;
    private long nextGC;

    @JsonCreator
    public FileQueue4Sink(
            @JsonProperty("path") String path,
            @JsonProperty("name") String name,
            @JsonProperty("gcPeriod") String gcPeriod) throws IOException {
        innerArray = new BigArrayImpl(path, name);
        // the ttl does not matter here since queue front index page is always cached
        this.queueFrontIndexPageFactory = new MappedPageFactoryImpl(QUEUE_FRONT_INDEX_PAGE_SIZE,
                ((BigArrayImpl)innerArray).getArrayDirectory() + QUEUE_FRONT_INDEX_PAGE_FOLDER,
                10 * 1000/*does not matter*/);
        IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);

        ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
        long front = queueFrontIndexBuffer.getLong();
        queueFrontIndex.set(front);

        consumedIndex = front;

        this.gcPeriod = new Period(gcPeriod == null ? "PT1h" : gcPeriod);
        nextGC = new DateTime().plus(this.gcPeriod).getMillis();
    }

    @Override
    public void put(Message msg) {
        try {
            innerArray.append(serde.serialize(msg));
        } catch (IOException e) {
            log.error("Exception on put: " + e.getMessage(), e);
        }
    }

    @Override
    public int retrieve(int batchSize, List<Message> msgList) {
        int i = 0;
        long queueFrontIndex = -1;
        try {
            queueFrontWriteLock.lock();
            if (isEmpty()) {
                return 0;
            }

            queueFrontIndex = this.queueFrontIndex.get();
            // restore consumedIndex if not committed
            consumedIndex = queueFrontIndex;

            while (i < batchSize && consumedIndex < innerArray.getHeadIndex()) {
                msgList.add(serde.deserialize(innerArray.get(consumedIndex)));
                ++consumedIndex;
                ++i;
            }

            if (System.currentTimeMillis() > nextGC) {
                gc();
            }
        } catch (IOException e) {
            log.error("Exception on retrieve:" + e.getMessage(), e);
        } finally {
            queueFrontWriteLock.unlock();
        }

        return i;
    }

    private void gc() throws IOException {
        long beforeIndex = this.queueFrontIndex.get();
        if (beforeIndex == 0L) { // wrap
            beforeIndex = Long.MAX_VALUE;
        } else {
            beforeIndex--;
        }
        try {
            this.innerArray.removeBeforeIndex(beforeIndex);
        } catch (IndexOutOfBoundsException ex) {
            // ignore
        }
    }

    @Override
    public void commit() {
        try {
            queueFrontWriteLock.lock();
            this.queueFrontIndex.set(consumedIndex);
            // persist the queue front
            IMappedPage queueFrontIndexPage = null;
            queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
            queueFrontIndexBuffer.putLong(consumedIndex);
            queueFrontIndexPage.setDirty(true);
        } catch (IOException e) {
            log.error("IOException on commint: " + e.getMessage(), e);
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public void close() {
        try {
            gc();
            if (this.queueFrontIndexPageFactory != null) {
                this.queueFrontIndexPageFactory.releaseCachedPages();
            }

            this.innerArray.close();
        } catch(IOException e) {
            log.error("IOException while closing: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public long size() {
        return innerArray.getHeadIndex() - queueFrontIndex.get();
    }
}