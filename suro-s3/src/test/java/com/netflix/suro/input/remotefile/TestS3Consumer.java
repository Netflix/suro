package com.netflix.suro.input.remotefile;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.netflix.suro.input.RecordParser;
import com.netflix.suro.input.SuroInput;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.routing.MessageRouter;
import com.netflix.suro.sink.notice.Notice;
import com.netflix.util.Pair;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class TestS3Consumer {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private ObjectMapper jsonMapper = new DefaultObjectMapper();
    private final int testFileCount = 6;

    @Test
    public void test() throws Exception {
        final String downloadPath = tempDir.newFolder().getAbsolutePath();

        final CountDownLatch latch = new CountDownLatch(1);
        final ConcurrentSkipListSet<String> removedKeys = new ConcurrentSkipListSet<String>();
        final AtomicInteger count = new AtomicInteger(0);
        final AtomicInteger peekedMsgCount = new AtomicInteger(0);
        final AtomicInteger invalidMsgCount = new AtomicInteger(0);

        Notice<String> mockedNotice = new Notice<String>() {
            @Override
            public void init() {
            }

            @Override
            public boolean send(String message) {
                return false;
            }

            @Override
            public String recv() {
                return null;
            }

            @Override
            public Pair<String, String> peek() {
                if (peekedMsgCount.get() == 1) {
                    // return invalid msg
                    invalidMsgCount.incrementAndGet();
                    return new Pair<String, String>("receiptHandle" + peekedMsgCount.getAndIncrement(), "invalid_msg");
                }
                if (peekedMsgCount.get() == 3) {
                    // return invalid msg
                    invalidMsgCount.incrementAndGet();
                    return new Pair<String, String>("receiptHandle" + peekedMsgCount.getAndIncrement(), "{\n" +
                            "    \"Message\": {\n" +
                            "        \"s3Bucket\": \"bucket\",\n" +
                            "        \"s3ObjectKey\": \"key\"\n" +
                            "    }\n" +
                            "}");
                }
                if (peekedMsgCount.get() == 5) {
                    // return invalid msg
                    invalidMsgCount.incrementAndGet();
                    return new Pair<String, String>("receiptHandle" + peekedMsgCount.getAndIncrement(), "{\n" +
                            "    \"Message\": {\n" +
                            "        \"Bucket\": \"bucket\",\n" +
                            "        \"ObjectKey\": [\"key\"]\n" +
                            "    }\n" +
                            "}");
                }
                try {
                    List<String> dummyKeys = new ArrayList<String>();
                    dummyKeys.add("prefix/key" + (count.getAndIncrement()));
                    dummyKeys.add("prefix/key" + (count.getAndIncrement()));
                    return new Pair<String, String>(
                            "receiptHandle" + peekedMsgCount.getAndIncrement(),
                            jsonMapper.writeValueAsString(
                                    new ImmutableMap.Builder<String, Object>()
                                            .put("Message",
                                                    new ImmutableMap.Builder<String, Object>()
                                                            .put("s3Bucket", "bucket")
                                                            .put("s3ObjectKey", dummyKeys)
                                                            .build())
                                            .build()));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                } finally {
                    if (count.get() == testFileCount) {
                        latch.countDown();
                    }
                }
            }

            @Override
            public void remove(String key) {
                removedKeys.add(key);
            }

            @Override
            public String getStat() {
                return null;
            }
        };

        AWSCredentialsProvider awsCredentials = mock(AWSCredentialsProvider.class);
        AWSCredentials credentials = mock(AWSCredentials.class);
        doReturn("accessKey").when(credentials).getAWSAccessKeyId();
        doReturn("secretKey").when(credentials).getAWSSecretKey();
        doReturn(credentials).when(awsCredentials).getCredentials();

        MessageRouter router = mock(MessageRouter.class);

        int numOfLines = 3;
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numOfLines; ++i) {
            sb.append("line" + i).append('\n');
        }

        RestS3Service s3 = mock(RestS3Service.class);
        doAnswer(new Answer<S3Object>() {
            @Override
            public S3Object answer(InvocationOnMock invocation) throws Throwable {
                S3Object obj = mock(S3Object.class);
                doReturn(new StringInputStream(sb.toString())).when(obj).getDataInputStream();
                return obj;
            }
        }).when(s3).getObject(anyString(), anyString());

        RecordParser recordParser = mock(RecordParser.class);
        List<MessageContainer> messages = new ArrayList<MessageContainer>();
        int numOfMessages = 5;
        for (int i = 0; i < numOfMessages; ++i) {
            messages.add(mock(MessageContainer.class));
        }
        doReturn(messages).when(recordParser).parse(anyString());

        S3Consumer consumer = new S3Consumer(
                "id",
                "s3Endpoint",
                mockedNotice,
                1000,
                3,
                downloadPath,
                recordParser,
                awsCredentials,
                router,
                jsonMapper,
                s3);

        consumer.start();
        latch.await();
        consumer.shutdown();

        verify(router, times(numOfMessages * numOfLines * count.get())).process(any(SuroInput.class), any(MessageContainer.class));
        assertEquals(removedKeys.size(), peekedMsgCount.get() - invalidMsgCount.get());

        // no files under downloadPath
        assertEquals(new File(downloadPath).list().length, 0);
    }
}
