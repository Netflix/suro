package com.netflix.suro.sink.remotefile;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.SerDe;
import com.netflix.suro.nofify.Notify;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.localfile.FileNameFormatter;
import com.netflix.suro.sink.localfile.LocalFileSink;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.acl.gs.GSAccessControlList;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.ProviderCredentials;
import org.jets3t.service.utils.MultipartUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class S3FileSink implements Sink {
    public static final String TYPE = "S3FileSink";

    static Logger log = LoggerFactory.getLogger(S3FileSink.class);

    private final String name;
    private final LocalFileSink localFileSink;
    private final String bucket;
    private final long maxPartSize;

    private final Notify notify;
    private final RemotePrefixFormatter prefixFormatter;

    @JacksonInject("multipartUtils")
    private MultipartUtils mpUtils;
    @JacksonInject("credentials")
    private ProviderCredentials credentials;

    private RestS3Service s3Service;
    private final ExecutorService uploader;

    @JsonCreator
    public S3FileSink(
            @JsonProperty("name") String name,
            @JsonProperty("localFileSink") LocalFileSink localFileSink,
            @JsonProperty("bucket") String bucket,
            @JsonProperty("maxPartSize") long maxPartSize,
            @JsonProperty("concurrentUpload") int concurrentUpload,
            @JsonProperty("notify") Notify notify,
            @JsonProperty("prefixFormatter") RemotePrefixFormatter prefixFormatter) {
        this.name = name;
        this.localFileSink = localFileSink;
        this.bucket = bucket;
        this.maxPartSize = maxPartSize;
        this.prefixFormatter = prefixFormatter;

        this.notify = notify;

        uploader = Executors.newFixedThreadPool(concurrentUpload + 1);

        Preconditions.checkNotNull(name, "name is needed");
        Preconditions.checkNotNull(localFileSink, "localFileSink is needed");
        Preconditions.checkNotNull(bucket, "bucket is needed");
        Preconditions.checkArgument(maxPartSize > 0, "maxPartSize is needed");
        Preconditions.checkArgument(concurrentUpload > 0, "concurrentUpload is needed");
        Preconditions.checkNotNull(notify, "notify is needed");
        Preconditions.checkNotNull(prefixFormatter, "prefixFormatter is needed");
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void writeTo(Message message, SerDe serde) {
        localFileSink.writeTo(message, serde);
    }

    private boolean running = false;
    @Override
    public void open() {
        if (mpUtils == null) {
            mpUtils = new MultipartUtils(maxPartSize);
        }
        try {
            s3Service = new RestS3Service(credentials);
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

    @Monitor(name="lastFileSize", type = DataSourceType.GAUGE)
    private long lastFileSize;
    @Monitor(name="uploadDuration", type = DataSourceType.GAUGE)
    private long uploadDuration;

    private void uploadFile(final String filePath) {
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
                    uploadDuration = t2 - t1;

                    doNotify(remoteFilePath, localFile.length());

                    localFile.delete();
                    log.info("upload done deleting from local: " + filePath);
                } catch (Throwable e) {
                    log.error("Exception while uploading: " + e.getMessage());
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

        notify.send(jsonMessage.toString());
    }

    private String makeUploadPath(File file) {
        return prefixFormatter.get() + file.getName();
    }
}