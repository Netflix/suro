package com.netflix.suro.sink.remotefile;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.serde.SerDe;
import com.netflix.suro.nofify.Notify;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.localfile.FileNameFormatter;
import com.netflix.suro.sink.localfile.LocalFileSink;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.acl.gs.GSAccessControlList;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.utils.MultipartUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class S3FileSink implements Sink {
    public static final String TYPE = "S3FileSink";

    static Logger log = LoggerFactory.getLogger(S3FileSink.class);

    private final String name;
    private final LocalFileSink localFileSink;
    private final String bucket;
    private final String s3Endpoint;
    private final long maxPartSize;

    private final Notify notify;
    private final RemotePrefixFormatter prefixFormatter;

    @JacksonInject("multipartUtils")
    private MultipartUtils mpUtils;
    @JacksonInject("credentials")
    private AWSCredentialsProvider credentialsProvider;

    private RestS3Service s3Service;
    private final ExecutorService uploader;

    @JsonCreator
    public S3FileSink(
            @JsonProperty("name") String name,
            @JsonProperty("localFileSink") LocalFileSink localFileSink,
            @JsonProperty("bucket") String bucket,
            @JsonProperty("s3Endpoint") String s3Endpoint,
            @JsonProperty("maxPartSize") long maxPartSize,
            @JsonProperty("concurrentUpload") int concurrentUpload,
            @JsonProperty("notify") Notify notify,
            @JsonProperty("prefixFormatter") RemotePrefixFormatter prefixFormatter) {
        this.name = name;
        this.localFileSink = localFileSink;
        this.bucket = bucket;
        this.s3Endpoint = s3Endpoint;
        this.maxPartSize = maxPartSize;
        this.prefixFormatter = prefixFormatter;

        this.notify = notify;

        uploader = Executors.newFixedThreadPool(concurrentUpload + 1);

        Preconditions.checkNotNull(name, "name is needed");
        Preconditions.checkNotNull(localFileSink, "localFileSink is needed");
        Preconditions.checkNotNull(bucket, "bucket is needed");
        Preconditions.checkNotNull(bucket, "s3Endpoint is needed");
        Preconditions.checkArgument(maxPartSize > 0, "maxPartSize is needed");
        Preconditions.checkArgument(concurrentUpload > 0, "concurrentUpload is needed");
        Preconditions.checkNotNull(notify, "notify is needed");
        Preconditions.checkNotNull(prefixFormatter, "prefixFormatter is needed");
    }

    @Override
    public void writeTo(Message message, SerDe serde) {
        localFileSink.writeTo(message, serde);
    }

    private boolean running = false;
    @Override
    public void open() {
        if (mpUtils == null) { // not injected
            mpUtils = new MultipartUtils(maxPartSize);
        }
        try {
            Jets3tProperties properties = new Jets3tProperties();
            properties.setProperty("s3service.s3-endpoint", s3Endpoint);
            s3Service = new RestS3Service(
                    new AWSSessionCredentialsAdapter(credentialsProvider), null, null, properties);
        } catch (S3ServiceException e) {
            throw new RuntimeException(e);
        }

        running = true;

        uploader.submit(new Runnable() {
            @Override
            public void run() {
                while (running) {
                    String note = localFileSink.recvNotify();
                    if (note != null) {
                        uploadFile(note);
                    }
                }
                String note = localFileSink.recvNotify();
                while (note != null) {
                    uploadFile(note);
                    note = localFileSink.recvNotify();
                }
            }
        });
        localFileSink.open();
    }

    @Override
    public void close() {
        try {
            running = false;
            localFileSink.close();
            uploader.shutdown();
            uploader.awaitTermination(60000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // ignore exceptions while closing
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
    private AtomicInteger uploadedFileCount = new AtomicInteger(0);

    private Set<String> processingFileSet = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    private BlockingQueue<String> processedFileQueue = new LinkedBlockingQueue<String>();
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

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
                    long t2 = System.currentTimeMillis();

                    log.info("upload duration: " + (t2 - t1) + " ms " +
                            "for " + filePath + " Len: " + localFile.length() + " bytes");
                    lastFileSize = localFile.length();
                    uploadedFileCount.incrementAndGet();
                    uploadDuration = t2 - t1;

                    doNotify(remoteFilePath, localFile.length());
                    deleteFile(filePath);

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

    private void deleteFile(String filePath) {
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

    private void doNotify(String filePath, long fileSize) throws JSONException {
        JSONObject jsonMessage = new JSONObject();
        jsonMessage.put("bucket", bucket);
        jsonMessage.put("filePath", filePath);
        jsonMessage.put("size", fileSize);
        jsonMessage.put("collector", FileNameFormatter.localHostAddr);

        if (notify.send(jsonMessage.toString()) == false) {

        }
    }

    private String makeUploadPath(File file) {
        return prefixFormatter.get() + file.getName();
    }
}