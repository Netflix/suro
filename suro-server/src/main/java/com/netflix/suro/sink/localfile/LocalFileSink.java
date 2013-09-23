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
import com.netflix.suro.sink.QueuedSink;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.queue.MemoryQueue4Sink;
import com.netflix.suro.sink.queue.MessageQueue4Sink;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class LocalFileSink extends QueuedSink implements Sink {
    static Logger log = LoggerFactory.getLogger(LocalFileSink.class);

    public static final String TYPE = "local";

    public static final String suffix = ".suro";
    public static final String done = ".done";

    private final String outputDir;
    private final FileWriter writer;
    private final long maxFileSize;
    private final Period rotationPeriod;
    private final int minPercentFreeDisk;
    private final Notify<String> notify;

    private QueueManager queueManager;
    private SpaceChecker spaceChecker;

    private String filePath;
    private long nextRotation;

    @Monitor(name = "writtenMessages", type = DataSourceType.COUNTER)
    private long writtenMessages;
    @Monitor(name = "writtenBytes", type = DataSourceType.COUNTER)
    private long writtenBytes;
    @Monitor(name = "errorClosedFiles", type = DataSourceType.COUNTER)
    private long errorClosedFiles;

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
        this.queueManager = queueManager;
        this.spaceChecker = spaceChecker;

        Monitors.registerObject(LocalFileSink.class.getSimpleName() + "-" + outputDir, this);
        initialize(queue4Sink == null ? new MemoryQueue4Sink(10000) : queue4Sink, batchSize, batchTimeout);
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

    @Override
    public void writeTo(Message message) {
        queue4Sink.offer(message);
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

        renameNotify(filePath);

        filePath = newName;

        nextRotation = new DateTime().plus(rotationPeriod).getMillis();

        if (spaceChecker.hasEnoughSpace() == false) {
            queueManager.stopTakingTraffic();
        } else {
            queueManager.startTakingTraffic();
        }

        messageWrittenInRotation = false;
    }

    @Override
    protected void beforePolling() throws IOException {
        // Don't rotate if we are not running
        if (isRunning &&
                (writer.getLength() > maxFileSize ||
                        System.currentTimeMillis() > nextRotation)) {
            rotate();
        }
    }

    @Override
    protected void write(List<Message> msgList) throws IOException {
        for (Message msg : msgList) {
            writer.writeTo(msg);
            DynamicCounter.increment(
                    MonitorConfig.builder("writtenMessages")
                            .withTag(TagKey.DATA_SOURCE, msg.getRoutingKey())
                            .build());
            ++writtenMessages;
            DynamicCounter.increment(
                    MonitorConfig.builder("writtenBytes")
                            .withTag(TagKey.DATA_SOURCE, msg.getRoutingKey())
                            .build(), msg.getPayload().length);
            writtenBytes += msg.getPayload().length;

            messageWrittenInRotation = true;
        }

        writer.sync();
        queue4Sink.commit();
        msgList.clear();
        lastBatch = System.currentTimeMillis();
    }

    @Override
    protected void innerClose() throws IOException {
        writer.close();
        renameNotify(filePath);
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

    public void cleanUp() {
        try {
            FileSystem fs = writer.getFS();
            FileStatus[] files = fs.listStatus(new Path(outputDir));
            for (FileStatus file: files) {
                String fileName = file.getPath().getName();
                if (fileName.endsWith(done)) {
                    notify.send(outputDir + fileName);
                } else if (fileName.endsWith(suffix)) {
                    long lastPeriod =
                            new DateTime().minus(rotationPeriod).minus(rotationPeriod).getMillis();
                    if (file.getModificationTime() < lastPeriod) {
                        ++errorClosedFiles;
                        log.error(outputDir + fileName + " is not closed properly!!!");
                        String doneFile = fileName.replace(suffix, done);
                        writer.setDone(outputDir + fileName, outputDir + doneFile);
                        notify.send(outputDir + doneFile);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Exception while on cleanUp: " + e.getMessage(), e);
        }
    }

    @Override
    public String getStat() {
        return String.format(
                "%d msgs, %s written",
                writtenMessages,
                FileUtils.byteCountToDisplaySize(writtenBytes));
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
