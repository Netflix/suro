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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageSerDe;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * File based {@link MessageQueue4Sink}, delegating actual logic to {@link FileBlockingQueue}
 *
 * @author jbae
 */
@NotThreadSafe
public class FileQueue4Sink implements MessageQueue4Sink {
    private static final Logger log = LoggerFactory.getLogger(FileQueue4Sink.class);

    public static final String TYPE = "file";

    private final FileBlockingQueue<Message> queue;

    @JsonCreator
    public FileQueue4Sink(
            @JsonProperty("path") String path,
            @JsonProperty("name") String name,
            @JsonProperty("gcPeriod") String gcPeriod,
            @JsonProperty("sizeLimit") long sizeLimit) throws IOException {

        queue = new FileBlockingQueue<Message>(
                path,
                name,
                new Period(gcPeriod == null ? "PT1m" : gcPeriod).toStandardSeconds().getSeconds(),
                new MessageSerDe(),
                sizeLimit == 0 ? Long.MAX_VALUE : sizeLimit);
    }

    @Override
    public boolean offer(Message msg) {
        try {
            return queue.offer(msg);
        } catch (Exception e) {
            log.error("Exception on offer: " + e.getMessage(), e);
            return false;
        }
    }

    @Override
    public int drain(int batchSize, List<Message> msgList) {
        return queue.drainTo(msgList, batchSize);
    }

    @Override
    public Message poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    @Override
    public void close() {
        queue.close();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public long size() {
        return queue.size();
    }

    @Override
    public long remainingCapacity() {
        return Long.MAX_VALUE; // theoretically unlimited
    }
}