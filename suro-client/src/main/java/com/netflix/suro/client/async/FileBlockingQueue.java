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

package com.netflix.suro.client.async;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;
import com.netflix.governator.guice.lazy.LazySingleton;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.message.serde.SerDe;

import java.io.IOException;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@LazySingleton
public class FileBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {
    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    private final IBigQueue queue;
    private final SerDe<E> serDe;

    @Inject
    public FileBlockingQueue(ClientConfig config, SerDe<E> serDe) {
        try {
            queue = new BigQueueImpl(config.getAsyncFileQueuePath(), "suroClient");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.serDe = serDe;

        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    queue.gc();
                } catch (IOException e) {
                    // ignore exception while gc
                }
            }
        }, config.getAsyncFileQueueGCInterval(), config.getAsyncFileQueueGCInterval(), TimeUnit.SECONDS);
    }

    @Override
    public E poll() {
        if (isEmpty()) {
            return null;
        }
        E x = null;
        int c = -1;
        lock.lock();
        try {
            if (isEmpty() == false) {
                x = serDe.deserialize(queue.dequeue());
                if (isEmpty() == false) {
                    notEmpty.signal();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
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
            return serDe.deserialize(queue.peek());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(E e) {
        Preconditions.checkNotNull(e);
        try {
            queue.enqueue(serDe.serialize(e));
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
            x = serDe.deserialize(queue.dequeue());
            if (isEmpty() == false) {
                notEmpty.signal();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }

        return x;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E x = null;
        int c = -1;
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (isEmpty()) {
                if (nanos <= 0)
                    return null;
                nanos = notEmpty.awaitNanos(nanos);
            }
            x = serDe.deserialize(queue.dequeue());
            if (isEmpty() == false) {
                notEmpty.signal();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
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
        try {
            int n = Math.min(maxElements, size());
            // count.get provides visibility to first n Nodes
            int i = 0;
            while (i < n) {
                c.add(serDe.deserialize(queue.dequeue()));
                ++i;
            }
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
                return queue.isEmpty() == false;
            }

            @Override
            public E next() {
                try {
                    return serDe.deserialize(queue.dequeue());
                } catch (Exception e) {
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
        return (int) queue.size();
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