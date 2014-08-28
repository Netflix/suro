/*
 * Copyright 2013 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.suro.queue;

import com.google.common.base.Preconditions;
import com.leansoft.bigqueue.BigArrayImpl;
import com.leansoft.bigqueue.IBigArray;
import com.leansoft.bigqueue.page.IMappedPage;
import com.leansoft.bigqueue.page.IMappedPageFactory;
import com.leansoft.bigqueue.page.MappedPageFactoryImpl;
import com.netflix.suro.message.SerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * File based blocking queue with <a href="https://github.com/bulldog2011/bigqueue">BigQueue</a>
 * @param <E> Type name should be given and its SerDe should be implemented.
 *
 * With the argument path and name, files will be created under the directory
 * [path]/[name]. BigQueue needs to do garge collection, which is deleting
 * unnecessary page file. Garbage collection is done in the background every
 * gcPeriodInSec seconds.
 *
 * When the messages are retrieved from the queue,
 * we can control the behavior whether to remove messages immediately or wait
 * until we commit. autoCommit true means removing messages immediately.
 *
 * @author jbae
 */
public class FileBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {
    private static final Logger log = LoggerFactory.getLogger(FileBlockingQueue.class);

    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

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

    private final SerDe<E> serDe;
    private final long sizeLimit;

    public FileBlockingQueue(
            String path,
            String name,
            int gcPeriodInSec,
            SerDe<E> serDe) throws IOException {
        this(path, name, gcPeriodInSec, serDe, Long.MAX_VALUE);
    }

    public FileBlockingQueue(
            String path,
            String name,
            int gcPeriodInSec,
            SerDe<E> serDe,
            long sizeLimit) throws IOException {
        innerArray = new BigArrayImpl(path, name);
        // the ttl does not matter here since queue front index page is always cached
        this.queueFrontIndexPageFactory = new MappedPageFactoryImpl(QUEUE_FRONT_INDEX_PAGE_SIZE,
                ((BigArrayImpl)innerArray).getArrayDirectory() + QUEUE_FRONT_INDEX_PAGE_FOLDER,
                10 * 1000/*does not matter*/);
        IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);

        ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
        queueFrontIndex.set(queueFrontIndexBuffer.getLong());

        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    gc();
                } catch (Exception e) {
                    log.error("Exception while gc: " + e.getMessage(), e);
                }
            }
        }, gcPeriodInSec, gcPeriodInSec, TimeUnit.SECONDS);

        this.serDe = serDe;
        this.sizeLimit = sizeLimit;
    }

    public void gc() throws IOException {
        long beforeIndex = queueFrontIndex.get();
        if (beforeIndex > 0L) { // wrap
            beforeIndex--;
            try {
                innerArray.removeBeforeIndex(beforeIndex);
            } catch (IndexOutOfBoundsException e) {
                // do nothing
            }
        }

        try {
            innerArray.limitBackFileSize(sizeLimit);
            if (innerArray.getTailIndex() > queueFrontIndex.get()) {
                queueFrontIndex.set(innerArray.getTailIndex());
            }
        } catch (IndexOutOfBoundsException e) {
            // do nothing
        }
    }

    private void commitInternal() throws IOException {
        // persist the queue front
        IMappedPage queueFrontIndexPage = null;
        queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
        ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
        queueFrontIndexBuffer.putLong(queueFrontIndex.get());
        queueFrontIndexPage.setDirty(true);
    }

    public void close() {
        try {
            gc();
            if (queueFrontIndexPageFactory != null) {
                queueFrontIndexPageFactory.releaseCachedPages();
            }

            innerArray.close();
        } catch(IOException e) {
            log.error("IOException while closing: " + e.getMessage(), e);
        }
    }

    @Override
    public E poll() {
        if (isEmpty()) {
            return null;
        }
        E x = null;
        lock.lock();
        try {
            if (!isEmpty()) {
                x = consumeElement();
                if (!isEmpty()) {
                    notEmpty.signal();
                }
            }
        } catch (IOException e) {
            log.error("IOException while poll: " + e.getMessage(), e);
            return null;
        } finally {
            lock.unlock();
        }

        return x;
    }

    @Override
    public E peek() {
        if (isEmpty()) {
            return null;
        }
        lock.lock();
        try {
            return consumeElement();
        } catch (IOException e) {
            log.error("IOException while peek: " + e.getMessage(), e);
            return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(E e) {
        Preconditions.checkNotNull(e);
        try {
            innerArray.append(serDe.serialize(e));
            signalNotEmpty();
            return true;
        } catch (IOException ex) {
            return false;
        }
    }

    @Override
    public void put(E e) throws InterruptedException {
        offer(e);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        return offer(e);
    }

    @Override
    public E take() throws InterruptedException {
        E x;
        lock.lockInterruptibly();
        try {
            while (isEmpty()) {
                notEmpty.await();
            }
            x = consumeElement();
        } catch (IOException e) {
            log.error("IOException on take: " + e.getMessage(), e);
            return null;
        } finally {
            lock.unlock();
        }

        return x;
    }

    private E consumeElement() throws IOException {
        // restore consumedIndex if not committed
        E x = serDe.deserialize(innerArray.get(queueFrontIndex.getAndIncrement()));
        commitInternal();
        return x;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E x = null;
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (isEmpty()) {
                if (nanos <= 0)
                    return null;
                nanos = notEmpty.awaitNanos(nanos);
            }
            x = consumeElement();
        } catch (IOException e) {
            log.error("IOException on poll: " + e.getMessage(), e);
            return null;
        } finally {
            lock.unlock();
        }

        return x;
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        if (c == null)
            throw new NullPointerException();
        if (c == this)
            throw new IllegalArgumentException();

        lock.lock();
        // restore consumedIndex if not committed
        try {
            int n = Math.min(maxElements, size());
            // count.get provides visibility to first n Nodes
            int i = 0;
            while (i < n && queueFrontIndex.get() < innerArray.getHeadIndex()) {
                c.add(serDe.deserialize(innerArray.get(queueFrontIndex.getAndIncrement())));
                ++i;
            }
            commitInternal();
            return n;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Iterator<E> iterator() {
        return new Iterator<E>() {

            @Override
            public boolean hasNext() {
                return !isEmpty();
            }

            @Override
            public E next() {
                try {
                    E x = consumeElement();
                    return x;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove is not supported, use dequeue()");
            }
        };
    }

    @Override
    public int size() {
        return (int)(innerArray.getHeadIndex() - queueFrontIndex.get());
    }

    private void signalNotEmpty() {
        lock.lock();
        try {
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }
}