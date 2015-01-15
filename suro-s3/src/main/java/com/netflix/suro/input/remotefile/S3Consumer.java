package com.netflix.suro.input.remotefile;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.suro.input.RecordParser;
import com.netflix.suro.input.SuroInput;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.routing.MessageRouter;
import com.netflix.suro.sink.notice.Notice;
import com.netflix.suro.sink.remotefile.AWSSessionCredentialsAdapter;
import com.netflix.util.Pair;
import org.apache.commons.io.FileUtils;
import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

public class S3Consumer implements SuroInput {
    public static final String TYPE = "s3";

    private static Logger log = LoggerFactory.getLogger(S3Consumer.class);

    private final String id;
    private final String s3Endpoint;
    private final long timeout;
    private final int concurrentDownload;
    private final Notice<String> notice;
    private final RecordParser recordParser;
    private final String downloadPath;
    private AWSCredentialsProvider credentialsProvider;
    private RestS3Service s3Service;
    private volatile boolean running = false;
    private ExecutorService executor;
    private Future<?> runner = null;

    private final MessageRouter router;
    private final ObjectMapper jsonMapper;

    @JsonCreator
    public S3Consumer(
            @JsonProperty("id") String id,
            @JsonProperty("s3Endpoint") String s3Endpoint,
            @JsonProperty("notice") Notice notice,
            @JsonProperty("recvTimeout") long timeout,
            @JsonProperty("concurrentDownload") int concurrentDownload,
            @JsonProperty("downloadPath") String downloadPath,
            @JsonProperty("recordParser") RecordParser recordParser,
            @JacksonInject AWSCredentialsProvider credentialProvider,
            @JacksonInject MessageRouter router,
            @JacksonInject ObjectMapper jsonMapper,
            @JacksonInject RestS3Service s3Service
    ) {
        this.id = id;
        this.s3Endpoint = s3Endpoint == null ? "s3.amazonaws.com" : s3Endpoint;
        this.notice = notice;
        this.timeout = timeout == 0 ? 1000 : timeout;
        this.concurrentDownload = concurrentDownload == 0 ? 5 : concurrentDownload;
        this.recordParser = recordParser;
        this.downloadPath = downloadPath == null ? "/logs/suro-s3consumer/" + id : downloadPath;

        this.credentialsProvider = credentialProvider;
        this.router = router;
        this.jsonMapper = jsonMapper;

        this.s3Service = s3Service;

        Preconditions.checkNotNull(notice, "notice is needed");
        Preconditions.checkNotNull(recordParser, "recordParser is needed");
    }

    @Override
    public String getId() {
        return id;
    }

    private static final long MAX_PAUSE = 10000;

    @Override
    public void start() throws Exception {
        if (s3Service == null) {
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
        }

        executor = new ThreadPoolExecutor(
                concurrentDownload + 1,
                concurrentDownload + 1,
                0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(concurrentDownload) {
                    @Override
                    public boolean offer(Runnable runnable) {
                        try {
                            put(runnable); // not to reject the task, slowing down
                        } catch (InterruptedException e) {
                            // do nothing
                        }
                        return true;
                    }
                },
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("S3Consumer-" + id + "-%d").build());

        notice.init();

        running = true;
        runner = executor.submit(new Runnable() {
            @Override
            public void run() {
                while (running) {
                    try {
                        long pause = Math.min(pausedTime.get(), MAX_PAUSE);
                        if (pause > 0) {
                            Thread.sleep(pause);
                            pausedTime.set(0);
                        }
                        Pair<String, String> msg = notice.peek();
                        if (msg != null) {
                            executor.submit(createDownloadRunnable(msg));
                        } else {
                            Thread.sleep(timeout);
                        }
                    } catch (Exception e) {
                        log.error("Exception on receiving messages from Notice", e);
                    }
                }
            }
        });
    }

    @Override
    public void shutdown() {
        try {
            log.info("shutting down S3Consumer now");

            running = false;
            try {
                runner.get();
            } catch (InterruptedException e) {
                // do nothing
            } catch (ExecutionException e) {
                log.error("Exception on stopping the task", e);
            }

            executor.shutdown();
            while (true) {
                if (!executor.awaitTermination(timeout * 5, TimeUnit.MILLISECONDS)) {
                    log.warn("downloading jobs were not terminated gracefully, retry again...");
                } else {
                    break;
                }
            }

            s3Service.shutdown();
        } catch (Exception e) {
            log.error("Exception on shutting down s3Service: " + e.getMessage(), e);
        }
    }

    private AtomicLong pausedTime = new AtomicLong(0);

    @Override
    public void setPause(long ms) {
        pausedTime.addAndGet(ms);
    }

    public static TypeReference<Map<String, Object>> typeReference = new TypeReference<Map<String, Object>>() {};
    private static final int retryCount = 5;
    private static final int sleepOnS3Exception = 5000;

    private Runnable createDownloadRunnable(final Pair<String, String> msg) {
        Map<String, Object> msgMap = null;

        try {
            msgMap = parseMessage(msg);
        } catch (Exception e) {
            log.error("Invalid message: " + e.getMessage(), e);
            return createEmptyRunnable(msg);
        }

        String s3Bucket = null;
        List<String> s3ObjectKey = null;
        try {
            s3Bucket = (String) msgMap.get("s3Bucket");
            s3ObjectKey = (List<String>) msgMap.get("s3ObjectKey");
            if (s3Bucket == null || s3ObjectKey == null) {
                throw new NullPointerException("s3Bucket or s3ObjectKey is null");
            }
        } catch (Exception e) {
            log.error("Invalid message: " + e.getMessage(), e);
            return createEmptyRunnable(msg);
        }

        final String s3BucketClone = s3Bucket;
        final List<String> s3ObjectKeyClone = s3ObjectKey;

        return new Runnable() {
            @Override
            public void run() {
                List<String> downloadedFiles = new ArrayList<String>();

                for (String path : s3ObjectKeyClone) {
                    boolean success = false;
                    String localFileName = path.replace("/", "");
                    for (int i = 0; i < retryCount; ++i) {
                        try {
                            S3Object object = s3Service.getObject(s3BucketClone, path);
                            FileUtils.copyInputStreamToFile(object.getDataInputStream(),
                                    new File(downloadPath, localFileName));
                            success = true;
                            log.info(path + " downloaded successfully");
                            break;
                        } catch (Exception e) {
                            log.error("Exception on downloading and processing file: " + e.getMessage(), e);
                            DynamicCounter.increment(
                                    MonitorConfig.builder("s3Exception").withTag("consumerId", id).build());
                            try {
                                Thread.sleep(sleepOnS3Exception);
                            } catch (InterruptedException e1) {
                                // do nothing
                            }
                        }
                    }
                    if (success) {
                        downloadedFiles.add(localFileName);
                    }
                }

                if (s3ObjectKeyClone.size() == downloadedFiles.size()) {
                    for (String path : downloadedFiles) {
                        try {
                            BufferedReader br = new BufferedReader(
                                new InputStreamReader(
                                    createInputStream(path)));
                            String data = null;
                            while ((data = br.readLine()) != null) {
                                try {
                                    if (data.trim().length() > 0) {
                                        for (MessageContainer msg : recordParser.parse(data)) {
                                            router.process(S3Consumer.this, msg);
                                        }
                                    }
                                } catch (Exception e) {
                                    log.error("Exception on parsing and processing: " + e.getMessage(), e);
                                }
                            }
                            br.close();
                            deleteFile(path);
                        } catch (Exception e) {
                            log.error("Exception on processing downloaded file: " + e.getMessage(), e);
                            DynamicCounter.increment(
                                MonitorConfig.builder("processingException").withTag("consumerId", id).build()
                            );
                        }
                    }

                    notice.remove(msg.first());
                }
            }
        };
    }

    private void deleteFile(String path) {
        File f = new File(downloadPath, path);
        while (f.exists()) {
            f.delete();
        }
    }

    @VisibleForTesting
    protected Map<String, Object> parseMessage(Pair<String, String> msg) throws IOException {
        Map<String, Object> msgContainer = jsonMapper.readValue(msg.second(), typeReference);
        if (!(msgContainer.get("Message") instanceof Map)) {
            return jsonMapper.readValue(msgContainer.get("Message").toString(), typeReference);
        } else {
            return (Map<String, Object>) msgContainer.get("Message");
        }
    }

    private InputStream createInputStream(String path) throws IOException {
        if (path.endsWith(".gz")) {
            return new GZIPInputStream(
                    new FileInputStream(new File(downloadPath, path)));
        } else {
            return new FileInputStream(new File(downloadPath, path));
        }
    }

    private Runnable createEmptyRunnable(final Pair<String, String> msg) {
        return new Runnable() {

            @Override
            public void run() {
                log.error("invalid msg: " + msg.second());
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof S3Consumer) {
            S3Consumer kafkaConsumer = (S3Consumer) o;
            return kafkaConsumer.id.equals(id);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return (getId()).hashCode();
    }
}
