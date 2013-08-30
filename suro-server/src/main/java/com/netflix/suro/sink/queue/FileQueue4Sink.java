package com.netflix.suro.sink.queue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.leansoft.bigqueue.BigArrayImpl;
import com.leansoft.bigqueue.IBigArray;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.serde.MessageSerDe;
import com.netflix.suro.message.serde.SerDe;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.List;

@NotThreadSafe
public class FileQueue4Sink implements MessageQueue4Sink {
    static Logger log = LoggerFactory.getLogger(FileQueue4Sink.class);

    public static final String TYPE = "file";

    private IBigArray bigArray;
    private long index;
    private long consumedIndex;
    private SerDe<Message> serde = new MessageSerDe();
    private Period gcPeriod;
    private long nextGC;

    @JsonCreator
    public FileQueue4Sink(
            @JsonProperty("path") String path,
            @JsonProperty("name") String name,
            @JsonProperty("gcPeriod") String gcPeriod) throws IOException {
        bigArray = new BigArrayImpl(path, name);
        index = bigArray.getHeadIndex();
        consumedIndex = index;

        this.gcPeriod = new Period(gcPeriod == null ? "PT1h" : gcPeriod);
        nextGC = new DateTime().plus(this.gcPeriod).getMillis();
    }

    @Override
    public void put(Message msg) {
        try {
            bigArray.append(serde.serialize(msg));
        } catch (IOException e) {
            log.error("Exception on put: " + e.getMessage(), e);
        }
    }

    @Override
    public int retrieve(int batchSize, List<Message> msgList) {
        consumedIndex = index;
        int i = 0;
        try {
            while (i < batchSize && consumedIndex < bigArray.getHeadIndex()) {
                msgList.add(serde.deserialize(bigArray.get(consumedIndex)));
                ++consumedIndex;
                ++i;
            }

            if (System.currentTimeMillis() > nextGC) {
                gc();
            }
        } catch (IOException e) {
            log.error("Exception on retrieve:" + e.getMessage(), e);
        }

        return i;
    }

    private void gc() throws IOException {
        if (isEmpty() == false) {
            bigArray.removeBeforeIndex(index);
        }
    }

    @Override
    public void commit() {
        index = consumedIndex;
    }

    @Override
    public void close() {
        try {
            gc();
            bigArray.close();
        } catch (IOException e) {
            // ignore exception on close
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public long size() {
        return bigArray.getHeadIndex() - index;
    }
}