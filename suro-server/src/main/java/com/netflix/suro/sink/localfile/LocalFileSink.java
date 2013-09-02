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

package com.netflix.suro.sink.localfile;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.TagKey;
import com.netflix.suro.message.Message;
import com.netflix.suro.nofify.Notify;
import com.netflix.suro.nofify.QueueNotify;
import com.netflix.suro.queue.QueueManager;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.queue.MemoryQueue4Sink;
import com.netflix.suro.sink.queue.MessageQueue4Sink;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class LocalFileSink extends Thread implements Sink {
    static Logger log = LoggerFactory.getLogger(LocalFileSink.class);

    public static final String TYPE = "LocalFileSink";

    public static final String suffix = ".suro";
    public static final String done = ".done";

    private final String outputDir;
    private final FileWriter writer;
    private final long maxFileSize;
    private final Period rotationPeriod;
    private final int minPercentFreeDisk;
    private final Notify<String> notify;
    private final MessageQueue4Sink queue4Sink;
    private final int batchSize;
    private final int batchTimeout;

    private QueueManager queueManager;
    private SpaceChecker spaceChecker;

    private String fileName;
    private long nextRotation;

    @Monitor(name = "writtenMessages", type = DataSourceType.COUNTER)
    private AtomicLong writtenMessages = new AtomicLong(0);
    @Monitor(name = "writtenBytes", type = DataSourceType.COUNTER)
    private AtomicLong writtenBytes = new AtomicLong(0);

    private boolean messageWrittenInRotation = false;

    @JsonCreator
    public LocalFileSink(
            @JsonProperty("outputDir") String outputDir,
            @JsonProperty("writer") FileWriter writer,
            @JsonProperty("notify") Notify notify,
            @JsonProperty("maxFileSize") long maxFileSize,
            @JsonProperty("rotationPeriod") String rotationPeriod,
            @JsonProperty("minPercentFreeDisk") int minPercentFreeDisk,
            @JsonProperty("queue4Sink") MessageQueue4Sink queue4Sink,
            @JsonProperty("batchSize") int batchSize,
            @JsonProperty("batchTimeout") int batchTimeout,
            @JacksonInject("queueManager") QueueManager queueManager,
            @JacksonInject("spaceChecker") SpaceChecker spaceChecker) {
        if (outputDir.endsWith("/") == false) {
            outputDir += "/";
        }
        Preconditions.checkNotNull(outputDir, "outputDir is needed");

        this.outputDir = outputDir;
        this.writer = writer == null ? new TextFileWriter(null) : writer;
        this.maxFileSize = maxFileSize == 0 ? 100 * 1024 * 1024 : maxFileSize;
        this.rotationPeriod = new Period(rotationPeriod == null ? "PT1m" : rotationPeriod);
        this.minPercentFreeDisk = minPercentFreeDisk == 0 ? 50 : minPercentFreeDisk;
        this.notify = notify == null ? new QueueNotify<String>() : notify;
        this.queue4Sink = queue4Sink == null ? new MemoryQueue4Sink(10000) : queue4Sink;
        this.batchSize = batchSize == 0 ? 200 : batchSize;
        this.batchTimeout = batchTimeout == 0 ? 1000 : batchTimeout;
        this.queueManager = queueManager;
        this.spaceChecker = spaceChecker;
    }

    @Override
    public void open() {
        try {
            if (spaceChecker == null) {
                spaceChecker = new SpaceChecker(minPercentFreeDisk, outputDir);
            }
            if (queueManager == null) {
                queueManager = new QueueManager();
            }

            notify.init();

            writer.open(outputDir);
            setName(LocalFileSink.class.getSimpleName() + "-" + outputDir);
            start();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private long lastBatch = System.currentTimeMillis();
    private boolean isRunning;
    private boolean isStopped;

    @Override
    public void run() {
        isRunning = true;
        List<Message> msgList = new LinkedList<Message>();

        while (isRunning) {
            try {
                // Don't rotate if we are not running
                if (isRunning &&
                        (writer.getLength() > maxFileSize ||
                        System.currentTimeMillis() > nextRotation)) {
                    rotate();
                }

                Message msg = queue4Sink.poll(
                        Math.max(0, lastBatch + batchTimeout - System.currentTimeMillis()),
                        TimeUnit.MILLISECONDS);
                boolean expired = (msg == null);
                if (expired == false) {
                    msgList.add(msg);
                    queue4Sink.drain(batchSize - msgList.size(), msgList);
                }
                boolean full = (msgList.size() >= batchSize);
                if ((expired || full) && msgList.size() > 0) {
                    write(msgList);
                }
            } catch (Exception e) {
                log.error("Exception on running: " + e.getMessage(), e);
            }
        }
        log.info("Shutdown request exit loop ..., queue.size at exit time: " + queue4Sink.size());
        try {
            while (queue4Sink.isEmpty() == false) {
                if (queue4Sink.drain(batchSize, msgList) > 0) {
                    write(msgList);
                }
            }

            log.info("Shutdown request internalClose done ...");
        } catch (Exception e) {
            log.error("Exception on terminating: " + e.getMessage(), e);
        } finally {
            isStopped = true;
        }
    }

    private void write(List<Message> msgList) {
        try {
            for (Message msg : msgList) {
                writer.writeTo(msg);
                DynamicCounter.increment(
                        MonitorConfig.builder("writtenMessages")
                                .withTag(TagKey.APP, msg.getApp())
                                .withTag(TagKey.DATA_SOURCE, msg.getRoutingKey())
                                .build());
                writtenMessages.incrementAndGet();
                DynamicCounter.increment(
                        MonitorConfig.builder("writtenBytes")
                                .withTag(TagKey.APP, msg.getApp())
                                .withTag(TagKey.DATA_SOURCE, msg.getRoutingKey())
                                .build(), msg.getPayload().length);
                writtenBytes.addAndGet(msg.getPayload().length);

                messageWrittenInRotation = true;
            }

            writer.sync();
            queue4Sink.commit();
            msgList.clear();
            lastBatch = System.currentTimeMillis();
        } catch (Exception e) {
            log.error("Exception on write: " + e.getMessage(), e);
        }
    }

    @Override
    public void writeTo(Message message) {
        queue4Sink.put(message);
    }

    public static class SpaceChecker {
        private final int minPercentFreeDisk;
        private final File outputDir;

        @Monitor(name = "freeSpace", type = DataSourceType.GAUGE)
        private long freeSpace;

        public SpaceChecker(int minPercentFreeDisk, String outputDir) {
            this.minPercentFreeDisk = minPercentFreeDisk;
            this.outputDir = new File(outputDir);

            Monitors.registerObject(this);
        }

        public boolean hasEnoughSpace() {
            long totalSpace = outputDir.getTotalSpace();

            freeSpace = outputDir.getFreeSpace();
            long minFreeAvailable = (totalSpace * minPercentFreeDisk) / 100;
            if (freeSpace < minFreeAvailable) {
                return false;
            } else {
                return true;
            }
        }
    }

    private void rotate() throws IOException {
        String newName = FileNameFormatter.get(outputDir) + suffix;
        writer.rotate(newName);

        renameNotify(fileName);

        fileName = newName;

        nextRotation = new DateTime().plus(rotationPeriod).getMillis();

        if (spaceChecker.hasEnoughSpace() == false) {
            queueManager.stopTakingTraffic();
        } else {
            queueManager.startTakingTraffic();
        }

        messageWrittenInRotation = false;
    }

    @Override
    public void close() {
        isRunning = false;
        log.info("Starting to close");
        do {
            try {
                Thread.sleep(500);
            } catch (Exception ignored) {
                log.error("ignoring an exception on close");
            }
        } while (isStopped == false);

        try {
            writer.close();
            renameNotify(fileName);
        } catch (IOException e) {
            // ignore exceptions when closing
            log.error("Exception while closing: " + e.getMessage(), e);
        } finally {
            log.info("close finished");
        }
    }

    @Override
    public String recvNotify() {
        return notify.recv();
    }

    private void renameNotify(String filePath) throws IOException {
        if (filePath != null) {
            if (messageWrittenInRotation) {
                // if we have the previous file
                String doneFile = filePath.replace(suffix, done);
                writer.setDone(filePath, doneFile);
                notify.send(doneFile);
            } else {
                // delete it
                LocalFileSink.deleteFile(filePath);
                File f = new File(filePath);
                String crcName = "." + f.getName() + ".crc";
                LocalFileSink.deleteFile(f.getParent() + "/" + crcName);
            }
        }
    }

    @Override
    public String getStat() {
        return String.format(
                "%d msgs, %s bytes written",
                writtenMessages.get(),
                FileUtils.byteCountToDisplaySize(writtenBytes.get()));
    }

    public static void deleteFile(String filePath) {
        // with AWS EBS, sometimes deletion failure without any IOException was observed
        // To prevent the surplus files, let's iterate file deletion
        int retryCount = 1;
        while (new File(filePath).exists() && retryCount <= 5) {
            try {
                Thread.sleep(1000 * retryCount);
                new File(filePath).delete();
                ++retryCount;
            } catch (Exception e) {
                log.warn("Exception while deleting the file: " + e.getMessage(), e);
            }
        }
    }
}
