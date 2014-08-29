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

import com.google.inject.Inject;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.message.Message;
import com.netflix.suro.queue.FileQueue4Sink;
import com.netflix.suro.queue.MemoryQueue4Sink;
import com.netflix.suro.queue.MessageQueue4Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A simple proxy of the queues used by {@link AsyncSuroClient}. It determines whether to use
 * an in-memory queue or a file-backed queue based on user configuration.
 */
public class Queue4Client {
    private static final Logger logger = LoggerFactory.getLogger(Queue4Client.class);

    private MessageQueue4Sink queue;

    @Inject
    public Queue4Client(ClientConfig config) {
        if (config.getAsyncQueueType().equals("memory")) {
            queue = new MemoryQueue4Sink(config.getAsyncMemoryQueueCapacity());
        } else {
            try {
                createQueuePathIfNeeded(config.getAsyncFileQueuePath());
                queue = new FileQueue4Sink(
                        config.getAsyncFileQueuePath(),
                        config.getAsyncFileQueueName(),
                        config.getAsyncFileQueueGCPeriod(),
                        config.getFileQueueSizeLimit());
            } catch (IOException e) {
                throw new IllegalStateException("Exception on initializing Queue4Client: " + e.getMessage(), e);
            }
        }
    }

    private void createQueuePathIfNeeded(String queueDir) {
        File f = new File(queueDir);
        if(f.exists() && f.isDirectory()) {
            return;
        }

        if(f.exists() && f.isFile()) {
            throw new IllegalStateException(String.format("The given file queue location %s is not a directory. ", queueDir));
        }

        boolean created = f.mkdirs();
        if(!created) {
            throw new IllegalStateException("Failed to create the queue dir " + queueDir);
        }else {
            logger.info("The queue directory {} did not exist but is created", queueDir);
        }
    }

    public boolean offer(Message msg) {
        return queue.offer(msg);
    }

    public Message poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return queue.poll(timeout, timeUnit);
    }

    public int drain(int batchSize, List<Message> msgList) {
        return queue.drain(batchSize, msgList);
    }

    public void close() {
        queue.close();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public long size() {
        return queue.size();
    }
}
