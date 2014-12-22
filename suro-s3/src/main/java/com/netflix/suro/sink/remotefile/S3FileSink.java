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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.suro.sink.localfile.FileNameFormatter;
import com.netflix.suro.sink.localfile.LocalFileSink;
import com.netflix.suro.sink.notice.Notice;
import com.netflix.suro.sink.notice.QueueNotice;
import org.codehaus.jettison.json.JSONObject;
import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.acl.gs.GSAccessControlList;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.utils.MultipartUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sink for S3. Ths embeds local file sink. When local file sink rotates
 * the file, the file is uploaded to S3.
 *
 * @author jbae
 */
public class S3FileSink extends RemoteFileSink {
    public static final String TYPE = "s3";

    private static Logger log = LoggerFactory.getLogger(S3FileSink.class);

    private final String bucket;
    private final String s3Endpoint;
    private final long maxPartSize;

    private final Notice<String> notice;

    private MultipartUtils mpUtils;

    private AWSCredentialsProvider credentialsProvider;

    private RestS3Service s3Service;
    @VisibleForTesting
    protected GrantAcl grantAcl;

    private final String s3Acl;
    private final int s3AclRetries;

    @JsonCreator
    public S3FileSink(
            @JsonProperty("localFileSink") LocalFileSink localFileSink,
            @JsonProperty("bucket") String bucket,
            @JsonProperty("s3Endpoint") String s3Endpoint,
            @JsonProperty("maxPartSize") long maxPartSize,
            @JsonProperty("concurrentUpload") int concurrentUpload,
            @JsonProperty("notice") Notice notice,
            @JsonProperty("prefixFormatter") RemotePrefixFormatter prefixFormatter,
            @JsonProperty("batchUpload") boolean batchUpload,
            @JsonProperty("s3Acl") String s3Acl,
            @JsonProperty("s3AclRetries") int s3AclRetries,
            @JacksonInject MultipartUtils mpUtils,
            @JacksonInject AWSCredentialsProvider credentialProvider) {
        super(localFileSink, prefixFormatter, concurrentUpload, batchUpload);

        this.bucket = bucket;
        this.s3Endpoint = s3Endpoint == null ? "s3.amazonaws.com" : s3Endpoint;
        this.maxPartSize = maxPartSize == 0 ? 20 * 1024 * 1024 : maxPartSize;
        this.notice = notice == null ? new QueueNotice<String>() : notice;

        this.mpUtils = mpUtils;
        this.credentialsProvider = credentialProvider;

        this.s3Acl = s3Acl;
        this.s3AclRetries = s3AclRetries > 0 ? s3AclRetries : 5;

        Preconditions.checkNotNull(bucket, "bucket is needed");
    }

    protected void initialize() {
        if (mpUtils == null) { // not injected
            mpUtils = new MultipartUtils(maxPartSize);
        }
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

        grantAcl = new GrantAcl(s3Service, s3Acl, s3AclRetries == 0 ? 5 : s3AclRetries);

        notice.init();
    }

    @Override
    public String recvNotice() {
        return notice.recv();
    }

    @Override
    public long checkPause() {
        return localFileSink.checkPause();
    }

    @Monitor(name="fail_grantAcl", type=DataSourceType.COUNTER)
    private AtomicLong fail_grantAcl = new AtomicLong(0);
    public long getFail_grantAcl() { return fail_grantAcl.get(); }

    @Override
    protected void notify(String filePath, long fileSize) throws Exception {
        JSONObject jsonMessage = new JSONObject();
        jsonMessage.put("bucket", bucket);
        jsonMessage.put("filePath", filePath);
        jsonMessage.put("size", fileSize);
        jsonMessage.put("collector", FileNameFormatter.localHostAddr);

        if (!notice.send(jsonMessage.toString())) {
            throw new RuntimeException("Notice failed");
        }
    }

    @Override
    protected void upload(String localFilePath, String remoteFilePath) throws Exception {
        S3Object file = new S3Object(new File(localFilePath));
        file.setBucketName(bucket);
        file.setKey(remoteFilePath);
        file.setAcl(GSAccessControlList.REST_CANNED_BUCKET_OWNER_FULL_CONTROL);
        List objectsToUploadAsMultipart = new ArrayList();
        objectsToUploadAsMultipart.add(file);
        mpUtils.uploadObjects(bucket, s3Service, objectsToUploadAsMultipart, null);

        if (!grantAcl.grantAcl(file)) {
            throw new RuntimeException("Failed to set Acl");
        }
    }
}