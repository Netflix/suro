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

package com.netflix.suro.sink.notice;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.TagKey;
import com.netflix.util.Pair;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Memory based implementation of {@link com.netflix.suro.sink.notice.Notice}
 *
 * @param <E> type of notice
 *
 * @author jbae
 */
public class QueueNotice<E> implements Notice<E> {
    public static final String TYPE = "queue";

    private static final int DEFAULT_LENGTH = 100;
    private static final long DEFAULT_TIMEOUT = 1000;

    private final BlockingQueue<E> queue;
    private final long timeout;

    @Monitor(name = TagKey.SENT_COUNT, type = DataSourceType.COUNTER)
    private AtomicLong sentMessageCount = new AtomicLong(0);
    @Monitor(name = TagKey.RECV_COUNT, type = DataSourceType.COUNTER)
    private AtomicLong recvMessageCount = new AtomicLong(0);
    @Monitor(name = TagKey.LOST_COUNT, type = DataSourceType.COUNTER)
    private AtomicLong lostMessageCount = new AtomicLong(0);

    public QueueNotice() {
        queue = new LinkedBlockingQueue<E>(DEFAULT_LENGTH);
        timeout = DEFAULT_TIMEOUT;

        Monitors.registerObject(this);
    }

    @JsonCreator
    public QueueNotice(
            @JsonProperty("length") int length,
            @JsonProperty("recvTimeout") long timeout) {
        this.queue = new LinkedBlockingQueue<E>(length > 0 ? length : DEFAULT_LENGTH);
        this.timeout = timeout > 0 ? timeout : DEFAULT_TIMEOUT;
    }

    @Override
    public void init() {
    }

    @Override
    public boolean send(E message) {
        if (queue.offer(message)) {
            sentMessageCount.incrementAndGet();
            return true;
        } else {
            lostMessageCount.incrementAndGet();
            return false;
        }
    }

    @Override
    public E recv() {
        try {
            return queue.poll(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    @Override
    public Pair<String, E> peek() {
        return new Pair<String, E>("", queue.peek());
    }

    @Override
    public void remove(String key) {

    }

    @Override
    public String getStat() {
        return String.format("QueueNotice with the size: %d, sent : %d, received: %d, dropped: %d",
                queue.size(), sentMessageCount.get(), recvMessageCount.get(), lostMessageCount.get());
    }
}
