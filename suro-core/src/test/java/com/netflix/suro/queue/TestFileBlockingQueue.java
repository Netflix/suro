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

import com.netflix.suro.message.StringSerDe;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class TestFileBlockingQueue {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testOpenAndReadFromStart() throws IOException {
        final FileBlockingQueue<String> queue = getFileBlockingQueue();
        createFile(queue, 3000);

        int count = 0;
        for (String m : new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return queue.iterator();
            }
        }) {
            assertEquals(m, "testString" + count);
            ++count;
        }

        assertEquals(count, 3000);
    }

    private FileBlockingQueue<String> getFileBlockingQueue() throws IOException {
        return new FileBlockingQueue<String>(
                    tempDir.newFolder().getAbsolutePath(), "default", 3600, new StringSerDe(), true);
    }

    @Test
    public void testOpenAndReadFromMark() throws IOException {
        final FileBlockingQueue<String> queue = getFileBlockingQueue();
        createFile(queue, 3000);

        int count = 0;
        for (String m : new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return queue.iterator();
            }
        }) {
            assertEquals(m, "testString" + count);
            ++count;
        }

        assertEquals(count, 3000);

        count = 0;
        createFile(queue, 3000);
        for (String m : new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return queue.iterator();
            }
        }) {
            assertEquals(m, "testString" + count);
            ++count;
        }

        assertEquals(count, 3000);
    }

    @Test
    public void testPollWait() throws InterruptedException, IOException {
        final FileBlockingQueue<String> queue = getFileBlockingQueue();

        final AtomicLong start = new AtomicLong(System.currentTimeMillis());
        String m = queue.poll(1000, TimeUnit.MILLISECONDS);
        final AtomicLong duration = new AtomicLong(System.currentTimeMillis() - start.get());
        assertTrue(duration.get() >= 1000 && duration.get() <= 2000);
        assertNull(m);

        ExecutorService e = Executors.newFixedThreadPool(1);
        e.execute(new Runnable() {
            @Override
            public void run() {
                start.set(System.currentTimeMillis());
                String m = null;
                try {
                    m = queue.poll(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
                assertNotNull(m);
                duration.set(System.currentTimeMillis() - start.get());
            }
        });

        Thread.sleep(1000);
        queue.offer("testString");

        assertTrue(duration.get() < 4000);
    }

    private void createFile(FileBlockingQueue<String> queue, int count) throws IOException {
        for (int i = 0; i < count; ++i) {
            assertTrue(queue.offer("testString" + i));
        }
    }
}
