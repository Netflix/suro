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
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import com.netflix.suro.connection.TestConnectionPool;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageSetReader;
import com.netflix.suro.queue.QueueManager;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.localfile.LocalFileSink;
import com.netflix.suro.sink.localfile.LocalFileSink.SpaceChecker;
import com.netflix.suro.sink.localfile.LocalFileSuroPlugin;

import org.apache.commons.io.FileUtils;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.multi.s3.S3ServiceEventListener;
import org.jets3t.service.utils.MultipartUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

public class TestS3FileSink {
    private static final String testdir = "/tmp/surotest/tests3filesink";

    public static final String s3FileSink = "{\n" +
            "    \"type\": \"" + S3FileSink.TYPE + "\",\n" +
            "    \"localFileSink\": {\n" +
            "        \"type\": \"" + LocalFileSink.TYPE + "\",\n" +
            "        \"outputDir\": \"" + testdir + "\",\n" +
            "        \"writer\": {\n" +
            "            \"type\": \"text\"\n" +
            "        },\n" +
            //"        \"maxFileSize\": 10240,\n" +
            "        \"rotationPeriod\": \"PT1m\",\n" +
            "        \"minPercentFreeDisk\": 50,\n" +
            "        \"notify\": {\n" +
            "            \"type\": \"queue\"\n" +
            "        }\n" +
            "    },\n" +
            "    \"bucket\": \"s3bucket\",\n" +
            "    \"maxPartSize\": 10000,\n" +
            "    \"concurrentUpload\":5,\n" +
            "    \"notify\": {\n" +
            "        \"type\": \"queue\"\n" +
            "    },\n" +
            "    \"prefixFormatter\": {" +
            "    \"type\": \"DateRegionStack\",\n" +
            "    \"date\": \"YYYYMMDD\"}\n" +
            "}";

    @Before
    @After
    public void clean() throws IOException {
        FileUtils.deleteDirectory(new File(testdir));
    }

    @Test
    public void testDefaultParameters() throws Exception {
        Injector injector = Guice.createInjector(
                new RemoteFileSuroPlugin(),
                new LocalFileSuroPlugin(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(ObjectMapper.class).to(DefaultObjectMapper.class);
                        bind(AWSCredentialsProvider.class)
                            .annotatedWith(Names.named("credentials"))
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
                            doNothing().when(mpUtils).uploadObjects(
                                    any(String.class),
                                    any(RestS3Service.class),
                                    any(List.class),
                                    any(S3ServiceEventListener.class));
                            
                            bind(MultipartUtils.class)
                            .annotatedWith(Names.named("multipartUtils"))
                            .toInstance(mpUtils);
                        } catch (Exception e) {
                            Assert.fail(e.getMessage());
                        }
                        bind(QueueManager.class)
                            .annotatedWith(Names.named("queueManager"))
                            .toInstance(new QueueManager());
                        bind(SpaceChecker.class)
                            .annotatedWith(Names.named("spaceChecker"))
                            .toInstance(mock(LocalFileSink.SpaceChecker.class));

                    }
                }
            );
        
        final String s3FileSink = "{\n" +
                "    \"type\": \"" + S3FileSink.TYPE + "\",\n" +
                "    \"localFileSink\": {\n" +
                "        \"type\": \"" + LocalFileSink.TYPE + "\",\n" +
                "        \"outputDir\": \"" + testdir + "\"\n" +
                "    },\n" +
                "    \"bucket\": \"s3bucket\"\n" +
                "}";

        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        final Map<String, Object> injectables = Maps.newHashMap();

        Sink sink = mapper.readValue(s3FileSink, new TypeReference<Sink>(){});
        sink.open();

        for (Message m : new MessageSetReader(TestConnectionPool.createMessageSet(100000))) {
            sink.writeTo(m);
        }
        sink.close();

        // check every file uploaded, deleted, and notified
        File dir = new File(testdir);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File file, String name) {
                if (file.isFile() && file.getName().startsWith(".") == false) {
                    return true;
                } else {
                    return false;
                }
            }
        });
        assertEquals(files.length, 0);
        int count = 0;
        while (sink.recvNotify() != null) {
            ++count;
        }
        assertTrue(count > 0);
    }

    @Test
    public void test() throws Exception {
        Injector injector = Guice.createInjector(
                new RemoteFileSuroPlugin(),
                new LocalFileSuroPlugin(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(ObjectMapper.class).to(DefaultObjectMapper.class);
                        bind(AWSCredentialsProvider.class)
                            .annotatedWith(Names.named("credentials"))
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
                    }
                }
            );
        
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        final Map<String, Object> injectables = Maps.newHashMap();

        injectables.put("region", "eu-west-1");
        injectables.put("stack", "gps");

        injectables.put("credentials", new AWSCredentialsProvider() {
            @Override
            public com.amazonaws.auth.AWSCredentials getCredentials() {
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
                // do nothing
            }
        });

        MultipartUtils mpUtils = mock(MultipartUtils.class);
        doNothing().when(mpUtils).uploadObjects(
                any(String.class),
                any(RestS3Service.class),
                any(List.class),
                any(S3ServiceEventListener.class));
        injectables.put("multipartUtils", mpUtils);
        injectables.put("queueManager", new QueueManager());

        mapper.setInjectableValues(new InjectableValues() {
            @Override
            public Object findInjectableValue(
                    Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance
            ) {
                return injectables.get(valueId);
            }
        });
        Sink sink = mapper.readValue(s3FileSink, new TypeReference<Sink>(){});
        sink.open();

        for (Message m : new MessageSetReader(TestConnectionPool.createMessageSet(100000))) {
            sink.writeTo(m);
        }
        sink.close();

        // check every file uploaded, deleted, and notified
        File dir = new File(testdir);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File file, String name) {
                if (file.isFile() && file.getName().startsWith(".") == false) {
                   return true;
                } else {
                    return false;
                }
            }
        });
        assertEquals(files.length, 0);
        int count = 0;
        while (sink.recvNotify() != null) {
            ++count;
        }
        assertTrue(count > 0);
    }
}
