package com.netflix.suro.sink.queue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.suro.FileBlockingQueue;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.serde.MessageSerDe;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

@NotThreadSafe
public class FileQueue4Sink implements MessageQueue4Sink {
    static Logger log = LoggerFactory.getLogger(FileQueue4Sink.class);

    public static final String TYPE = "file";

    private final FileBlockingQueue<Message> queue;

    @JsonCreator
    public FileQueue4Sink(
            @JsonProperty("path") String path,
            @JsonProperty("name") String name,
            @JsonProperty("gcPeriod") String gcPeriod) throws IOException {
        queue = new FileBlockingQueue<Message>(
                path,
                name,
                new Period(gcPeriod == null ? "PT1h" : gcPeriod).toStandardSeconds().getSeconds(),
                new MessageSerDe(),
                false);
    }

    @Override
    public void put(Message msg) {
        try {
            queue.put(msg);
        } catch (Exception e) {
            log.error("Exception on put: " + e.getMessage(), e);
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
    public void commit() {
        queue.commit();
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
}