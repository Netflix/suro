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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.suro.connection.TestConnectionPool;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageSetReader;
import com.netflix.suro.message.StringMessage;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.localfile.LocalFileSink;
import com.netflix.suro.sink.localfile.LocalFileSink.SpaceChecker;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.multi.s3.S3ServiceEventListener;
import org.jets3t.service.utils.MultipartUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class TestS3FileSink {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testDefaultParameters() throws Exception {
        String testDir = tempDir.newFolder().getAbsolutePath();

        Injector injector = getInjector();
        
        final String s3FileSink = "{\n" +
                "    \"type\": \"" + S3FileSink.TYPE + "\",\n" +
                "    \"localFileSink\": {\n" +
                "        \"type\": \"" + LocalFileSink.TYPE + "\",\n" +
                "        \"outputDir\": \"" + testDir + "\"\n" +
                "    },\n" +
                "    \"bucket\": \"s3bucket\"\n" +
                "}";

        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);

        Sink sink = mapper.readValue(s3FileSink, new TypeReference<Sink>(){});
        sink.open();

        for (Message m : new MessageSetReader(TestConnectionPool.createMessageSet(100000))) {
            sink.writeTo(new StringMessage(m));
        }
        sink.close();

        // check every file uploaded, deleted, and notified
        File[] files = getFiles(testDir);
        assertEquals(files.length, 0);
        int count = 0;
        while (sink.recvNotice() != null) {
            ++count;
        }
        assertTrue(count > 0);
    }

    @Test
    public void test() throws Exception {
        String testDir = tempDir.newFolder().getAbsolutePath();

        final String s3FileSink = "{\n" +
                "    \"type\": \"" + S3FileSink.TYPE + "\",\n" +
                "    \"localFileSink\": {\n" +
                "        \"type\": \"" + LocalFileSink.TYPE + "\",\n" +
                "        \"outputDir\": \"" + testDir + "\",\n" +
                "        \"writer\": {\n" +
                "            \"type\": \"text\"\n" +
                "        },\n" +
                //"        \"maxFileSize\": 10240,\n" +
                "        \"rotationPeriod\": \"PT1m\",\n" +
                "        \"minPercentFreeDisk\": 50,\n" +
                "        \"notice\": {\n" +
                "            \"type\": \"queue\"\n" +
                "        }\n" +
                "    },\n" +
                "    \"bucket\": \"s3bucket\",\n" +
                "    \"maxPartSize\": 10000,\n" +
                "    \"concurrentUpload\":5,\n" +
                "    \"notice\": {\n" +
                "        \"type\": \"queue\"\n" +
                "    },\n" +
                "    \"prefixFormatter\": {" +
                "    \"type\": \"DateRegionStack\",\n" +
                "    \"date\": \"YYYYMMDD\"}\n" +
                "}";


        Injector injector = getInjector();
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);

        Sink sink = mapper.readValue(s3FileSink, new TypeReference<Sink>(){});
        sink.open();

        for (Message m : new MessageSetReader(TestConnectionPool.createMessageSet(100000))) {
            sink.writeTo(new StringMessage(m));
        }
        sink.close();

        // check every file uploaded, deleted, and notified
        File[] files = getFiles(testDir);
        assertEquals(files.length, 0);
        int count = 0;
        while (sink.recvNotice() != null) {
            ++count;
        }
        assertTrue(count > 0);
    }

    @Test
    public void testTooManyFiles() throws IOException {
        String testDir = tempDir.newFolder().getAbsolutePath();

        Injector injector = getInjector();

        final String s3FileSink = "{\n" +
                "    \"type\": \"" + S3FileSink.TYPE + "\",\n" +
                "    \"localFileSink\": {\n" +
                "        \"type\": \"" + LocalFileSink.TYPE + "\",\n" +
                "        \"outputDir\": \"" + testDir + "\"\n" +
                "    },\n" +
                "    \"bucket\": \"s3bucket\"\n" +
                "}";

        // pre-create many files
        new File(testDir).mkdir();
        for (int i = 0; i < 100; ++i) {
            createFile(testDir, i);
        }
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);

        Sink sink = mapper.readValue(s3FileSink, new TypeReference<Sink>(){});
        sink.open();

        sink.close();

        // check every file uploaded, deleted, and notified
        File[] files = getFiles(testDir);
        assertEquals(files.length, 0);
        int count = 0;
        while (sink.recvNotice() != null) {
            ++count;
        }
        assertEquals(count, 100);
    }

    private void createFile(String testDir, int i) throws IOException {
        File f = new File(testDir, "fileNo" + i + ".done");
        f.createNewFile();
        FileOutputStream o = new FileOutputStream(f);
        o.write("temporaryStringContents".getBytes());
        o.close();
    }

    @Test
    public void testUploadAll() throws IOException {
        String testDir = tempDir.newFolder().getAbsolutePath();

        Injector injector = getInjector();

        final String s3FileSink = "{\n" +
                "    \"type\": \"" + S3FileSink.TYPE + "\",\n" +
                "    \"localFileSink\": {\n" +
                "        \"type\": \"" + LocalFileSink.TYPE + "\",\n" +
                "        \"outputDir\": \"" + testDir + "\"\n" +
                "    },\n" +
                "    \"bucket\": \"s3bucket\",\n" +
                "    \"batchUpload\":true\n" +
                "}";

        // pre-create many files
        new File(testDir).mkdir();
        for (int i = 0; i < 100; ++i) {
            createFile(testDir, i);
        }
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);

        S3FileSink sink = mapper.readValue(s3FileSink, new TypeReference<Sink>(){});
        sink.open();
        assertEquals(sink.getNumOfPendingMessages(), 100);
        sink.uploadAll(testDir);

        // check every file uploaded, deleted, and notified
        int count = 0;
        while (sink.recvNotice() != null) {
            ++count;
        }
        assertEquals(count, 100);

        File[] files = getFiles(testDir);
        assertEquals(files.length, 0);

        assertEquals(sink.getNumOfPendingMessages(), 0);
    }

    @Test
    public void testAclFailure() throws IOException, ServiceException, InterruptedException {
        String testDir = tempDir.newFolder().getAbsolutePath();

        final String s3FileSink = "{\n" +
                "    \"type\": \"" + S3FileSink.TYPE + "\",\n" +
                "    \"localFileSink\": {\n" +
                "        \"type\": \"" + LocalFileSink.TYPE + "\",\n" +
                "        \"outputDir\": \"" + testDir + "\"\n" +
                "    },\n" +
                "    \"bucket\": \"s3bucket\"" +
                "}";

        Injector injector = getInjector();
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);

        S3FileSink sink = mapper.readValue(s3FileSink, new TypeReference<Sink>(){});
        GrantAcl grantAcl = mock(GrantAcl.class);
        when(grantAcl.grantAcl(any(S3Object.class))).thenReturn(false);
        sink.open();
        sink.grantAcl = grantAcl;

        for (Message m : new MessageSetReader(TestConnectionPool.createMessageSet(100000))) {
            sink.writeTo(new StringMessage(m));
        }
        sink.close();
        File[] files = getFiles(testDir);

        assertTrue(files.length > 0);
        int count = 0;
        while (sink.recvNotice() != null) {
            ++count;
        }
        assertEquals(count, 0);
    }

    private File[] getFiles(String testDir) {
        // check no file uploaded, deleted, and notified
        File dir = new File(testDir);
        return dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File file, String name) {
                if (!name.startsWith(".")) {
                    return true;
                } else {
                    return false;
                }
            }
        });
    }

    private Injector getInjector() {
        return Guice.createInjector(
            new SuroSinkPlugin(),
            new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(ObjectMapper.class).to(DefaultObjectMapper.class);
                        bind(AWSCredentialsProvider.class)
                            .toInstance(new AWSCredentialsProvider() {
                                @Override
                                public AWSCredentials getCredentials() {
                                    return new AWSCredentials() {
                                        @Override
                                        public String getAWSAccessKeyId() {
                                            return "accessKey";
                                        }

                                        @Override
                                        public String getAWSSecretKey() {
                                            return "secretKey";
                                        }
                                    };
                                }

                                @Override
                                public void refresh() {
                                }
                            });

                        MultipartUtils mpUtils = mock(MultipartUtils.class);
                        try {
                            doAnswer(new Answer() {
                                @Override
                                public Object answer(InvocationOnMock invocation) throws Throwable {
                                    Thread.sleep(1000);
                                    return null;
                                }
                            }).when(mpUtils).uploadObjects(
                                any(String.class),
                                any(RestS3Service.class),
                                any(List.class),
                                any(S3ServiceEventListener.class));

                            bind(MultipartUtils.class).toInstance(mpUtils);
                        } catch (Exception e) {
                            Assert.fail(e.getMessage());
                        }
                        bind(SpaceChecker.class).toInstance(mock(SpaceChecker.class));
                    }
            }
        );
    }
}
