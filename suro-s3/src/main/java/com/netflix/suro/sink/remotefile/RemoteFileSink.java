package com.netflix.suro.sink.remotefile;

import com.google.common.base.Preconditions;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.localfile.LocalFileSink;
import com.netflix.suro.sink.remotefile.formatter.DynamicRemotePrefixFormatter;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class RemoteFileSink implements Sink {
    private static final Logger log = LoggerFactory.getLogger(RemoteFileSink.class);

    protected final LocalFileSink localFileSink;
    private final RemotePrefixFormatter prefixFormatter;

    private final ExecutorService uploader;
    private final ExecutorService localFilePoller;
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private final boolean batchUpload;

    private boolean running = false;
    private static final int processingFileQueueThreshold = 1000;
    private static final String processingFileQueueCleanupInterval = "PT60s";

    private Set<String> processingFileSet = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    private BlockingQueue<String> processedFileQueue = new LinkedBlockingQueue<String>();

    public RemoteFileSink(
            LocalFileSink localFileSink,
            RemotePrefixFormatter prefixFormatter,
            int concurrentUpload,
            boolean batchUpload) {
        this.localFileSink = localFileSink;
        this.prefixFormatter = prefixFormatter == null ? new DynamicRemotePrefixFormatter("date(yyyyMMdd)") : prefixFormatter;
        this.batchUpload = batchUpload;

        Preconditions.checkNotNull(localFileSink, "localFileSink is needed");

        uploader = Executors.newFixedThreadPool(concurrentUpload == 0 ? 5 : concurrentUpload);
        localFilePoller = Executors.newSingleThreadExecutor();

        if (!batchUpload) {
            localFileSink.cleanUp(false);
        }

        Monitors.registerObject(
                this.getClass().getSimpleName() + '-' + localFileSink.getOutputDir().replace('/', '_'),
                this);
    }

    @Override
    public void writeTo(MessageContainer message) {
        localFileSink.writeTo(message);
    }

    @Override
    public void open() {
        initialize();

        if (!batchUpload) {
            running = true;

            localFilePoller.submit(new Runnable() {
                @Override
                public void run() {
                    while (running) {
                        uploadAllFromQueue();
                        localFileSink.cleanUp(false);
                    }
                    uploadAllFromQueue();
                }
            });
            localFileSink.open();

            int schedulingSecond = new Period(processingFileQueueCleanupInterval).toStandardSeconds().getSeconds();
            scheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    if (processingFileSet.size() > processingFileQueueThreshold) {
                        String file = null;
                        int count = 0;
                        while (processingFileSet.size() > processingFileQueueThreshold &&
                                (file = processedFileQueue.poll()) != null) {
                            processingFileSet.remove(file);
                            ++count;
                        }
                        log.info(count + " files are removed from processingFileSet");
                    }
                }
            }, schedulingSecond, schedulingSecond, TimeUnit.SECONDS);
        }
    }

    @Override
    public void close() {
        try {
            if (!batchUpload) {
                localFileSink.close();
                running = false;
                localFilePoller.shutdown();

                localFilePoller.awaitTermination(60000, TimeUnit.MILLISECONDS);
            }
            uploader.shutdown();
            uploader.awaitTermination(60000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            // ignore exceptions while closing
            log.error("Exception while closing: " + e.getMessage(), e);
        }
    }

    @Override
    public String getStat() {
        StringBuilder sb = new StringBuilder(localFileSink.getStat());
        sb.append('\n').append(String.format("%d files uploaded so far", uploadedFileCount.get()));

        return sb.toString();
    }

    public void uploadAll(String dir) {
        clearFileHistory();

        while (localFileSink.cleanUp(dir, true) > 0) {
            uploadAllFromQueue();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // do nothing
            }
        }
    }

    private void clearFileHistory() {
        processedFileQueue.clear();
        processingFileSet.clear();
    }

    private void uploadAllFromQueue() {
        String note = localFileSink.recvNotice();
        while (note != null) {
            uploadFile(note);
            note = localFileSink.recvNotice();
        }
    }

    private void uploadFile(final String filePath) {
        // to prevent multiple uploading in any situations
        final String key = filePath.substring(filePath.lastIndexOf("/"));
        if (processingFileSet.contains(key)) {
            return;
        }
        processingFileSet.add(key);

        uploader.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    File localFile = new File(filePath);
                    long fileLength = localFile.length();
                    if (fileLength == 0) {
                        log.warn("empty file: " + filePath + " is abandoned");
                        localFileSink.deleteFile(filePath);
                        return;
                    }
                    String remoteFilePath = makeUploadPath(localFile);

                    long t1 = System.currentTimeMillis();

                    upload(filePath, remoteFilePath);

                    long t2 = System.currentTimeMillis();

                    log.info("upload duration: " + (t2 - t1) + " ms " +
                            "for " + filePath + " Len: " + fileLength + " bytes");

                    uploadedFileSize.addAndGet(fileLength);
                    uploadedFileCount.incrementAndGet();
                    uploadDuration = t2 - t1;

                    RemoteFileSink.this.notify(remoteFilePath, fileLength);
                    localFileSink.deleteFile(filePath);

                    log.info("upload done deleting from local: " + filePath);
                } catch (Exception e) {
                    uploadFailureCount.incrementAndGet();
                    log.error("Exception while uploading: " + e.getMessage(), e);
                } finally {
                    // check the file was deleted or not
                    if (new File(filePath).exists()) {
                        // something error happened
                        // it should be done again
                        processingFileSet.remove(key);
                    } else {
                        processedFileQueue.add(key);
                    }
                }
            }
        });
    }

    private String makeUploadPath(File file) {
        return prefixFormatter.get() + file.getName();
    }

    @Monitor(name = "uploadedFileSize", type = DataSourceType.COUNTER)
    public long getUploadedFileSize() {
        return uploadedFileSize.get();
    }

    @Monitor(name = "uploadDuration", type = DataSourceType.GAUGE)
    private long uploadDuration;

    @Monitor(name = "uploadedFileCount", type = DataSourceType.COUNTER)
    public int getUploadedFileCount() {
        return uploadedFileCount.get();
    }

    @Monitor(name = "uploadFailureCount", type=DataSourceType.COUNTER)
    public int getUploadFailureCount() {
        return uploadFailureCount.get();
    }

    private AtomicLong uploadedFileSize = new AtomicLong(0);
    private AtomicInteger uploadedFileCount = new AtomicInteger(0);
    private AtomicInteger uploadFailureCount = new AtomicInteger(0);

    abstract void initialize();
    abstract void upload(String localFilePath, String remoteFilePath) throws Exception;
    abstract void notify(String filePath, long fileSize) throws Exception;

    @Override
    public long getNumOfPendingMessages() {
        long numMessages = localFileSink.getNumOfPendingMessages();
        if (numMessages == 0) {
            return localFileSink.cleanUp(true);
        } else {
            return numMessages;
        }
    }
}
