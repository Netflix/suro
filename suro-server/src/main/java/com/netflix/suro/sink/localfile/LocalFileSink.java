package com.netflix.suro.sink.localfile;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.SerDe;
import com.netflix.suro.nofify.Notify;
import com.netflix.suro.queue.QueueManager;
import com.netflix.suro.sink.Sink;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class LocalFileSink implements Sink {
    static Logger log = LoggerFactory.getLogger(LocalFileSink.class);

    public static final String TYPE = "LocalFileSink";

    public static final String suffix = ".suro";
    public static final String done = ".done";

    private final String name;
    private final String outputDir;
    private final FileWriter writer;
    private final long maxFileSize;
    private final Period rotationPeriod;
    private final int minPercentFreeDisk;
    private final Notify notify;

    @JacksonInject("queueManager")
    private QueueManager queueManager;

    private String fileName;
    private long nextRotation;

    @Monitor(name = "freeSpace", type = DataSourceType.GAUGE)
    private long freeSpace = 0;

    @JsonCreator
    public LocalFileSink(
            @JsonProperty("name") String name,
            @JsonProperty("outputDir") String outputDir,
            @JsonProperty("writer") FileWriter writer,
            @JsonProperty("maxFileSize") long maxFileSize,
            @JsonProperty("rotationPeriod") Period rotationPeriod,
            @JsonProperty("minPercentFreeDisk") int minPercentFreeDisk,
            @JsonProperty("notify") Notify notify) {
        this.name = name;
        if (outputDir.endsWith("/") == false) {
            outputDir += "/";
        }
        this.outputDir = outputDir;
        this.writer = writer;
        this.maxFileSize = maxFileSize;
        this.rotationPeriod = rotationPeriod;
        this.minPercentFreeDisk = minPercentFreeDisk;
        this.notify = notify;

        Preconditions.checkNotNull(name, "name is needed");
        Preconditions.checkNotNull(outputDir, "outputDir is needed");
        Preconditions.checkNotNull(writer, "writer is needed");
        Preconditions.checkArgument(maxFileSize > 0, "maxFileSize is needed");
        Preconditions.checkNotNull(rotationPeriod, "rotationPeriod is needed");
        Preconditions.checkNotNull(minPercentFreeDisk, "minPercentFreeDisk is needed");
        Preconditions.checkNotNull(notify, "notify is needed");
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void open() {
        try {
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
            if (writer.getLength() > maxFileSize ||
                    System.currentTimeMillis() > nextRotation) {
                rotate();
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static class SpaceChecker {

    }

    private void rotate() throws IOException {
        String newName = FileNameFormatter.get(outputDir) + suffix;
        writer.rotate(newName);

        renameNotify(fileName);

        fileName = newName;

        nextRotation = new DateTime().plus(rotationPeriod).getMillis();

        File directory4Space = new File(outputDir);
        long totalSpace = directory4Space.getTotalSpace();
        freeSpace = directory4Space.getFreeSpace();
        long minFreeAvailable = (totalSpace * minPercentFreeDisk) / 100;
        if (freeSpace < minFreeAvailable) {
            log.error("No space left on device, Bail out!");
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
}
