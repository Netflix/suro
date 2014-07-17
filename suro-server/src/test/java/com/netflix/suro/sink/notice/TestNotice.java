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

package com.netflix.suro.sink.notice;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.netflix.suro.SuroPlugin;
import com.netflix.suro.aws.PropertyAWSCredentialsProvider;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.sink.TestSinkManager;
import org.apache.commons.codec.binary.Base64;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Arrays;

import static junit.framework.Assert.assertNull;
import static junit.framework.TestCase.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class TestNotice {
    private Injector injector;

    @Before
    public void setup() {
        injector = Guice.createInjector(
                new SuroPlugin() {
                    @Override
                    protected void configure() {
                        this.addSinkType("TestSink", TestSinkManager.TestSink.class);

                        this.addNoticeType(NoNotice.TYPE, NoNotice.class);
                        this.addNoticeType(QueueNotice.TYPE, QueueNotice.class);
                        this.addNoticeType(SQSNotice.TYPE, SQSNotice.class);
                    }
                },
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(ObjectMapper.class).to(DefaultObjectMapper.class);
                        bind(AWSCredentialsProvider.class).to(PropertyAWSCredentialsProvider.class);

                        bind(AmazonSQSClient.class).toProvider(AmazonSQSClientProvider.class).asEagerSingleton();
                    }
                }
        );
    }

    public static class AmazonSQSClientProvider implements Provider<AmazonSQSClient> {

        @Override
        public AmazonSQSClient get() {
            AmazonSQSClient client = mock(AmazonSQSClient.class);
            doReturn(new SendMessageResult()).when(client).sendMessage(any(SendMessageRequest.class));

            ReceiveMessageResult result = new ReceiveMessageResult();
            result.setMessages(Arrays.asList(new Message[]{new Message().withBody("receivedMessage")}));
            doReturn(result).when(client).receiveMessage(any(ReceiveMessageRequest.class));
            doReturn(new GetQueueUrlResult().withQueueUrl("queueURL")).when(client).getQueueUrl(any(GetQueueUrlRequest.class));

            return client;
        }
    }

    @Test
    public void testQueue() throws IOException {
        String desc = "{\n" +
                "    \"type\": \"queue\"   \n" +
                "}";

        ObjectMapper mapper = injector.getInstance(DefaultObjectMapper.class);
        Notice queueNotice = mapper.readValue(desc, new TypeReference<Notice>(){});
        queueNotice.init();
        queueNotice.send("message");
        assertEquals(queueNotice.recv(), "message");
        assertNull(queueNotice.recv());
    }

    @Test
    public void testSQS() throws IOException {
        String desc = "{\n" +
                "    \"type\": \"sqs\",\n" +
                "    \"queues\": [\n" +
                "        \"queue1\"\n" +
                "    ],\n" +
                "    \"region\": \"us-east-1\",\n" +
                "    \"connectionTimeout\": 3000,\n" +
                "    \"maxConnections\": 3,\n" +
                "    \"socketTimeout\": 1000,\n" +
                "    \"maxRetries\": 3\n" +
                "}";

        SqsTest sqsTest = new SqsTest(desc).invoke();
        ArgumentCaptor<SendMessageRequest> captor = sqsTest.getCaptor();
        Notice queueNotice = sqsTest.getQueueNotice();

        assertEquals(captor.getValue().getMessageBody(), "message");
        assertEquals(captor.getValue().getQueueUrl(), "queueURL");

        assertEquals(queueNotice.recv(), "receivedMessage");
    }

    @Test
    public void testSQSBase64() throws IOException {
        String desc = "{\n" +
                "    \"type\": \"sqs\",\n" +
                "    \"queues\": [\n" +
                "        \"queue1\"\n" +
                "    ],\n" +
                "    \"enableBase64Encoding\": true,\n" +
                "    \"region\": \"us-east-1\",\n" +
                "    \"connectionTimeout\": 3000,\n" +
                "    \"maxConnections\": 3,\n" +
                "    \"socketTimeout\": 1000,\n" +
                "    \"maxRetries\": 3\n" +
                "}";

        SqsTest sqsTest = new SqsTest(desc).invoke();
        ArgumentCaptor<SendMessageRequest> captor = sqsTest.getCaptor();
        Notice queueNotice = sqsTest.getQueueNotice();

        assertEquals(captor.getValue().getMessageBody(), new String(Base64.encodeBase64("message".getBytes()),
                Charsets.UTF_8));
        assertEquals(captor.getValue().getQueueUrl(), "queueURL");

        assertEquals(queueNotice.recv(), new String(Base64.decodeBase64("receivedMessage".getBytes()), Charsets.UTF_8));
    }

    private class SqsTest {
        private String desc;
        private Notice queueNotice;
        private ArgumentCaptor<SendMessageRequest> captor;

        public SqsTest(String desc) {
            this.desc = desc;
        }

        public Notice getQueueNotice() {
            return queueNotice;
        }

        public ArgumentCaptor<SendMessageRequest> getCaptor() {
            return captor;
        }

        public SqsTest invoke() throws IOException {
            ObjectMapper mapper = injector.getInstance(DefaultObjectMapper.class);
            AmazonSQSClient client = injector.getInstance(AmazonSQSClient.class);

            queueNotice = mapper.readValue(desc, new TypeReference<Notice>() {});
            queueNotice.init();
            queueNotice.send("message");
            captor = ArgumentCaptor.forClass(SendMessageRequest.class);
            verify(client).sendMessage(captor.capture());
            return this;
        }
    }
}
