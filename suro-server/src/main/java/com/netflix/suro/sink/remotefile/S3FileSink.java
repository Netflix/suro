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

package com.netflix.suro.sink.remotefile;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.suro.message.Message;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.localfile.FileNameFormatter;
import com.netflix.suro.sink.localfile.LocalFileSink;
import com.netflix.suro.sink.nofify.QueueNotify;
import com.netflix.suro.sink.notify.Notify;
import com.netflix.suro.sink.remotefile.formatter.SimpleDateFormatter;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.acl.gs.GSAccessControlList;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.utils.MultipartUtils;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class S3FileSink implements Sink {
    public static final String TYPE = "s3";

    static Logger log = LoggerFactory.getLogger(S3FileSink.class);

    private final LocalFileSink localFileSink;
    private final boolean batchUpload;

    private final String bucket;
    private final String s3Endpoint;
    private final long maxPartSize;

    private final Notify<String> notify;
    private final RemotePrefixFormatter prefixFormatter;

    private MultipartUtils mpUtils;

    private AWSCredentialsProvider credentialsProvider;

    private RestS3Service s3Service;
    private GrantAcl grantAcl;
    private final ExecutorService uploader;
    private final ExecutorService localFilePoller;

    private final String s3Acl;
    private final int s3AclRetries;

    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    @JsonCreator
    public S3FileSink(
            @JsonProperty("localFileSink") LocalFileSink localFileSink,
            @JsonProperty("bucket") String bucket,
            @JsonProperty("s3Endpoint") String s3Endpoint,
            @JsonProperty("maxPartSize") long maxPartSize,
            @JsonProperty("concurrentUpload") int concurrentUpload,
            @JsonProperty("notify") Notify notify,
            @JsonProperty("prefixFormatter") RemotePrefixFormatter prefixFormatter,
            @JsonProperty("batchUpload") boolean batchUpload,
            @JsonProperty("s3Acl") String s3Acl,
            @JsonProperty("s3AclRetries") int s3AclRetries,
            @JacksonInject("multipartUtils") MultipartUtils mpUtils,
            @JacksonInject("credentials") AWSCredentialsProvider credentialProvider) {
        this.localFileSink = localFileSink;
        this.bucket = bucket;
        this.s3Endpoint = s3Endpoint == null ? "s3.amazonaws.com" : s3Endpoint;
        this.maxPartSize = maxPartSize == 0 ? 20 * 1024 * 1024 : maxPartSize;
        this.notify = notify == null ? new QueueNotify<String>() : notify;
        this.prefixFormatter = prefixFormatter == null ? new SimpleDateFormatter("'P'yyyyMMdd'T'HHmmss") : prefixFormatter;
        this.batchUpload = batchUpload;

        this.mpUtils = mpUtils;
        this.credentialsProvider = credentialProvider;

        if (concurrentUpload == 0) {
            concurrentUpload = 5;
        }
        uploader = Executors.newFixedThreadPool(concurrentUpload);
        localFilePoller = Executors.newSingleThreadExecutor();

        this.s3Acl = s3Acl;
        this.s3AclRetries = s3AclRetries > 0 ? s3AclRetries : 5;

        Preconditions.checkNotNull(localFileSink, "localFileSink is needed");
        Preconditions.checkNotNull(bucket, "bucket is needed");

        if (batchUpload == false) {
            localFileSink.cleanUp();
        }
    }

    // testing purpose only
    public void setGrantAcl(GrantAcl grantAcl) {
        this.grantAcl = grantAcl;
    }

    @Override
    public void writeTo(Message message) {
        localFileSink.writeTo(message);
    }

    private boolean running = false;
    private static final int processingFileQueueThreshold = 1000;
    private static final String processingFileQueueCleanupInterval = "PT60s";

    @Override
    public void open() {
        if (mpUtils == null) { // not injected
            mpUtils = new MultipartUtils(maxPartSize);
        }
        try {
            Jets3tProperties properties = new Jets3tProperties();
            properties.setProperty("s3service.s3-endpoint", s3Endpoint);
            if (credentialsProvider.getCredentials() instanceof AWSSessionCredentials) {
                s3Service = new RestS3Service(
                        new AWSSessionCredentialsAdapter(credentialsProvider),
                        null, null, properties);
            } else {
                s3Service = new RestS3Service(
                        new AWSCredentials(
                                credentialsProvider.getCredentials().getAWSAccessKeyId(),
                                credentialsProvider.getCredentials().getAWSSecretKey()),
                        null, null, properties);
            }
        } catch (S3ServiceException e) {
            throw new RuntimeException(e);
        }

        grantAcl = new GrantAcl(s3Service, s3Acl, s3AclRetries == 0 ? 5 : s3AclRetries);

        notify.init();

        if (batchUpload == false) {
            running = true;

            localFilePoller.submit(new Runnable() {
                @Override
                public void run() {
                    while (running) {
                        uploadAllFromQueue();
                        localFileSink.cleanUp();
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

    private void clearFileHistory() {
        processedFileQueue.clear();
        processingFileSet.clear();
    }

    private void uploadAllFromQueue() {
        String note = localFileSink.recvNotify();
        while (note != null) {
            uploadFile(note);
            note = localFileSink.recvNotify();
        }
    }

    @Override
    public void close() {
        try {
            localFileSink.close();
            running = false;
            localFilePoller.shutdown();

            localFilePoller.awaitTermination(60000, TimeUnit.MILLISECONDS);
            uploader.shutdown();
            uploader.awaitTermination(60000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            // ignore exceptions while closing
            log.error("Exception while closing: " + e.getMessage(), e);
        }
    }

    @Override
    public String recvNotify() {
        return notify.recv();
    }

    @Override
    public String getStat() {
        StringBuilder sb = new StringBuilder(localFileSink.getStat());
        sb.append('\n').append(String.format("%d files uploaded so far", uploadedFileCount.get()));

        return sb.toString();
    }

    @Monitor(name = "lastFileSize", type = DataSourceType.GAUGE)
    private long lastFileSize;
    @Monitor(name = "uploadDuration", type = DataSourceType.GAUGE)
    private long uploadDuration;
    @Monitor(name = "uploadedFileCount", type = DataSourceType.COUNTER)
    public int getUploadedFileCount() {
        return uploadedFileCount.get();
    }
    private AtomicInteger uploadedFileCount = new AtomicInteger(0);

    @Monitor(name="fail_grantAcl", type=DataSourceType.COUNTER)
    private AtomicLong fail_grantAcl = new AtomicLong(0);
    public long getFail_grantAcl() { return fail_grantAcl.get(); }

    private Set<String> processingFileSet = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    private BlockingQueue<String> processedFileQueue = new LinkedBlockingQueue<String>();

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
                    String remoteFilePath = makeUploadPath(localFile);

                    long t1 = System.currentTimeMillis();
                    S3Object file = new S3Object(new File(filePath));
                    file.setBucketName(bucket);
                    file.setKey(remoteFilePath);
                    file.setAcl(GSAccessControlList.REST_CANNED_BUCKET_OWNER_FULL_CONTROL);
                    List objectsToUploadAsMultipart = new ArrayList();
                    objectsToUploadAsMultipart.add(file);
                    mpUtils.uploadObjects(bucket, s3Service, objectsToUploadAsMultipart, null);

                    if (grantAcl.grantAcl(file) == false) {
                        throw new RuntimeException("Failed to set Acl");
                    }

                    long t2 = System.currentTimeMillis();

                    log.info("upload duration: " + (t2 - t1) + " ms " +
                            "for " + filePath + " Len: " + localFile.length() + " bytes");
                    lastFileSize = localFile.length();
                    uploadedFileCount.incrementAndGet();
                    uploadDuration = t2 - t1;

                    doNotify(remoteFilePath, localFile.length());
                    LocalFileSink.deleteFile(filePath);

                    log.info("upload done deleting from local: " + filePath);
                } catch (Throwable e) {
                    log.error("Exception while uploading: " + e.getMessage());
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

    private void doNotify(String filePath, long fileSize) throws JSONException {
        JSONObject jsonMessage = new JSONObject();
        jsonMessage.put("bucket", bucket);
        jsonMessage.put("filePath", filePath);
        jsonMessage.put("size", fileSize);
        jsonMessage.put("collector", FileNameFormatter.localHostAddr);

        if (notify.send(jsonMessage.toString()) == false) {
            throw new RuntimeException("Notification failed");
        }
    }

    private String makeUploadPath(File file) {
        return prefixFormatter.get() + file.getName();
    }

    public void uploadAll(String dir) {
        clearFileHistory();

        while (localFileSink.cleanUp(dir) > 0) {
            uploadAllFromQueue();
        }
    }

    public boolean isBatchUpload() {
        return batchUpload;
    }

}