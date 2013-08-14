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
import com.netflix.suro.message.serde.SerDe;
import com.netflix.suro.nofify.Notify;
import com.netflix.suro.nofify.QueueNotify;
import com.netflix.suro.queue.QueueManager;
import com.netflix.suro.sink.Sink;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class LocalFileSink implements Sink {
    static Logger log = LoggerFactory.getLogger(LocalFileSink.class);

    public static final String TYPE = "LocalFileSink";

    public static final String suffix = ".suro";
    public static final String done = ".done";

    private final String outputDir;
    private final FileWriter writer;
    private final long maxFileSize;
    private final Period rotationPeriod;
    private final int minPercentFreeDisk;
    private final Notify notify;

    private final QueueManager queueManager;
    private SpaceChecker spaceChecker;

    private String fileName;
    private long nextRotation;

    @Monitor(name = "writtenMessages", type = DataSourceType.COUNTER)
    private AtomicLong writtenMessages = new AtomicLong(0);
    @Monitor(name = "writtenBytes", type = DataSourceType.COUNTER)
    private AtomicLong writtenBytes = new AtomicLong(0);

    @JsonCreator
    public LocalFileSink(
            @JsonProperty("outputDir") String outputDir,
            @JsonProperty("writer") FileWriter writer,
            @JsonProperty("notify") Notify notify,
            @JsonProperty("maxFileSize") long maxFileSize,
            @JsonProperty("rotationPeriod") String rotationPeriod,
            @JsonProperty("minPercentFreeDisk") int minPercentFreeDisk,
            @JacksonInject("queueManager") QueueManager queueManager,
            @JacksonInject("spaceChecker") SpaceChecker spaceChecker) {
        if (outputDir.endsWith("/") == false) {
            outputDir += "/";
        }
        this.outputDir = outputDir;
        this.writer = writer == null ? new TextFileWriter(null) : writer;
        this.maxFileSize = maxFileSize == 0 ? 100 * 1024 * 1024 : maxFileSize;
        this.rotationPeriod = new Period(rotationPeriod == null ? "PT1m" : rotationPeriod);
        this.minPercentFreeDisk = minPercentFreeDisk == 0 ? 50 : minPercentFreeDisk;
        this.notify = notify == null ? new QueueNotify() : notify;
        this.queueManager = queueManager;
        this.spaceChecker = spaceChecker;

        Preconditions.checkNotNull(outputDir, "outputDir is needed");
    }

    @Override
    public void open() {
        try {
            if (spaceChecker == null) {
                spaceChecker = new SpaceChecker(minPercentFreeDisk, outputDir);
            }
            if (queueManager == null) {
                throw new NullPointerException("queueManager should be injected");
            }
            writer.open(outputDir);
            rotate();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeTo(Message message, SerDe serde) {
        try {
            writer.writeTo(message, serde);
            DynamicCounter.increment(
                    MonitorConfig.builder("writtenMessages")
                            .withTag(TagKey.APP, message.getApp())
                            .withTag(TagKey.DATA_SOURCE, message.getRoutingKey())
                            .build());
            DynamicCounter.increment(
                    MonitorConfig.builder("writtenBytes")
                            .withTag(TagKey.APP, message.getApp())
                            .withTag(TagKey.DATA_SOURCE, message.getRoutingKey())
                            .build(), message.getPayload().length);

            if (writer.getLength() > maxFileSize ||
                    System.currentTimeMillis() > nextRotation) {
                rotate();
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
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
    }

    @Override
    public void close() {
        try {
            writer.close();
            renameNotify(fileName);
        } catch (IOException e) {
            // ignore exceptions when closing
            log.error("Exception while closing: " + e.getMessage(), e);
        }
    }

    @Override
    public String recvNotify() {
        return notify.recv();
    }

    private void renameNotify(String fileName) throws IOException {
        if (fileName != null) {
            // if we have the previous file
            String doneFile = fileName.replace(suffix, done);
            writer.setDone(fileName, doneFile);
            notify.send(doneFile);
        }
    }

    @Override
    public String getStat() {
        return String.format(
                "%d msgs, %d bytes written",
                writtenMessages.get(),
                FileUtils.byteCountToDisplaySize(writtenBytes.get()));
    }
}
