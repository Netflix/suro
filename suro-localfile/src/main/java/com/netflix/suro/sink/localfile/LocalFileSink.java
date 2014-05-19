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
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.sink.notice.QueueNotice;
import com.netflix.suro.sink.notice.Notice;
import com.netflix.suro.queue.TrafficController;
import com.netflix.suro.sink.QueuedSink;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.queue.MemoryQueue4Sink;
import com.netflix.suro.queue.MessageQueue4Sink;
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

/**
 * LocalFileSink appends messages to the file in local file system and rotates
 * the file when the file size reaches to the threshold or in the regular basis
 * whenever it comes earlier. When {@link com.netflix.suro.sink.localfile.LocalFileSink.SpaceChecker} checks not enough disk
 * space, it triggers {@link com.netflix.suro.queue.TrafficController} not to take the traffic anymore.
 *
 * @author jbae
 */
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
    private final Notice<String> notice;

    private TrafficController trafficController;
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
            @JsonProperty("notice") Notice notice,
            @JsonProperty("maxFileSize") long maxFileSize,
            @JsonProperty("rotationPeriod") String rotationPeriod,
            @JsonProperty("minPercentFreeDisk") int minPercentFreeDisk,
            @JsonProperty("queue4Sink") MessageQueue4Sink queue4Sink,
            @JsonProperty("batchSize") int batchSize,
            @JsonProperty("batchTimeout") int batchTimeout,
            @JacksonInject("queueManager") TrafficController trafficController,
            @JacksonInject("spaceChecker") SpaceChecker spaceChecker) {
        if (!outputDir.endsWith("/")) {
            outputDir += "/";
        }
        Preconditions.checkNotNull(outputDir, "outputDir is needed");

        this.outputDir = outputDir;
        this.writer = writer == null ? new TextFileWriter(null) : writer;
        this.maxFileSize = maxFileSize == 0 ? 200 * 1024 * 1024 : maxFileSize;
        this.rotationPeriod = new Period(rotationPeriod == null ? "PT2m" : rotationPeriod);
        this.minPercentFreeDisk = minPercentFreeDisk == 0 ? 85 : minPercentFreeDisk;
        this.notice = notice == null ? new QueueNotice<String>() : notice;
        this.trafficController = trafficController;
        this.spaceChecker = spaceChecker;

        Monitors.registerObject(LocalFileSink.class.getSimpleName() + "-" + outputDir.replace('/', '_'), this);
        initialize(queue4Sink == null ? new MemoryQueue4Sink(10000) : queue4Sink, batchSize, batchTimeout);
    }

    public String getOutputDir() {
        return outputDir;
    }

    @Override
    public void open() {
        try {
            if (spaceChecker == null) {
                spaceChecker = new SpaceChecker(minPercentFreeDisk, outputDir);
            }
            if (trafficController == null) {
                trafficController = new TrafficController() {
                    @Override
                    public void stopTakingTraffic() {

                    }

                    @Override
                    public void startTakingTraffic() {

                    }

                    @Override
                    public int getStatus() {
                        return 0;
                    }
                };
            }

            notice.init();

            writer.open(outputDir);
            setName(LocalFileSink.class.getSimpleName() + "-" + outputDir);
            start();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeTo(MessageContainer message) {
        enqueue(message.getMessage());
    }

    public static class SpaceChecker {
        private final int minPercentFreeDisk;
        private final File outputDir;

        @Monitor(name = "freeSpace", type = DataSourceType.GAUGE)
        private long freeSpace;

        /**
         * When the disk free space percentage becomes less than minPercentFreeDisk
         * we should stop taking the traffic.
         *
         * @param minPercentFreeDisk minimum percentage of free space
         * @param outputDir
         */
        public SpaceChecker(int minPercentFreeDisk, String outputDir) {
            this.minPercentFreeDisk = minPercentFreeDisk;
            this.outputDir = new File(outputDir);

            Monitors.registerObject(this);
        }

        /**
         *
         * @return true when the local disk on output directory has enough space, otherwise false
         */
        public boolean hasEnoughSpace() {
            long totalSpace = outputDir.getTotalSpace();

            freeSpace = outputDir.getFreeSpace();
            long minFreeAvailable = (totalSpace * minPercentFreeDisk) / 100;
            return freeSpace >= minFreeAvailable;
        }
    }

    private void rotate() throws IOException {
        String newName = FileNameFormatter.get(outputDir) + suffix;
        writer.rotate(newName);

        renameAndNotify(filePath);

        filePath = newName;

        nextRotation = new DateTime().plus(rotationPeriod).getMillis();

        if (!spaceChecker.hasEnoughSpace()) {
            trafficController.stopTakingTraffic();
        } else {
            trafficController.startTakingTraffic();
        }
    }

    /**
     * Before polling messages from the queue, it should check whether to rotate
     * the file and start to write to new file.
     *
     * @throws java.io.IOException
     */
    @Override
    protected void beforePolling() throws IOException {
        // Don't rotate if we are not running
        if (isRunning &&
                (writer.getLength() > maxFileSize ||
                        System.currentTimeMillis() > nextRotation)) {
            rotate();
        }
    }

    /**
     * Write all messages in msgList to file writer, sync the file,
     * commit the queue and clear messages
     *
     * @param msgList
     * @throws java.io.IOException
     */
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
    }

    @Override
    protected void innerClose() throws IOException {
        writer.close();
        renameAndNotify(filePath);
    }

    @Override
    public String recvNotice() {
        return notice.recv();
    }

    private void renameAndNotify(String filePath) throws IOException {
        if (filePath != null) {
            if (messageWrittenInRotation) {
                // if we have the previous file
                String doneFile = filePath.replace(suffix, done);
                writer.setDone(filePath, doneFile);
                notice.send(doneFile);
            } else {
                // delete it
                deleteFile(filePath);
            }
        }

        messageWrittenInRotation = false;
    }

    /**
     * This method calls {@link #cleanUp(String)} with outputDir
     *
     * @return
     */
    public int cleanUp() {
        return cleanUp(outputDir);
    }

    /**
     * List all files under the directory. If the file is marked as done, the
     * notice for that file would be sent. Otherwise, it checks the file
     * is not closed properly, the file is marked as done and the notice
     * would be sent. That file would cause EOFException when reading.
     *
     * @param dir
     * @return the number of files found in the directory
     */
    public int cleanUp(String dir) {
        if (!dir.endsWith("/")) {
            dir += "/";
        }

        int count = 0;

        try {
            FileSystem fs = writer.getFS();
            FileStatus[] files = fs.listStatus(new Path(dir));
            for (FileStatus file: files) {
                if (file.getLen() > 0) {
                    String fileName = file.getPath().getName();
                    String fileExt = getFileExt(fileName);
                    if (fileExt != null && fileExt.equals(done)) {
                        notice.send(dir + fileName);
                        ++count;
                    } else if (fileExt != null) {
                        long lastPeriod =
                                new DateTime().minus(rotationPeriod).minus(rotationPeriod).getMillis();
                        if (file.getModificationTime() < lastPeriod) {
                            ++errorClosedFiles;
                            log.error(dir + fileName + " is not closed properly!!!");
                            String doneFile = fileName.replace(fileExt, done);
                            writer.setDone(dir + fileName, dir + doneFile);
                            notice.send(dir + doneFile);
                            ++count;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Exception while on cleanUp: " + e.getMessage(), e);
        }

        return count;
    }

    /**
     * Simply returns file extension from the file path
     *
     * @param fileName
     * @return
     */
    public static String getFileExt(String fileName) {
        int dotPos = fileName.lastIndexOf('.');
        if (dotPos != -1 && dotPos != fileName.length() - 1) {
            return fileName.substring(dotPos);
        } else {
            return null;
        }
    }

    @Override
    public String getStat() {
        return String.format(
                "%d msgs, %s written",
                writtenMessages,
                FileUtils.byteCountToDisplaySize(writtenBytes));
    }

    private static final int deleteFileRetryCount = 5;

    /**
     *  With AWS EBS, sometimes deletion failure without any IOException was
     *  observed To prevent the surplus files, let's iterate file deletion.
     *  By default, it will try for five times.
     *
     * @param filePath
     */
    public void deleteFile(String filePath) {
        int retryCount = 1;
        while (retryCount <= deleteFileRetryCount) {
            try {
                if (writer.getFS().exists(new Path(filePath))) {
                    Thread.sleep(1000 * retryCount);
                    writer.getFS().delete(new Path(filePath), false);
                    ++retryCount;
                } else {
                    break;
                }
            } catch (Exception e) {
                log.warn("Exception while deleting the file: " + e.getMessage(), e);
            }
        }
    }
}
