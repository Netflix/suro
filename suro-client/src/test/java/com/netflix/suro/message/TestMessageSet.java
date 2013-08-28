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

package com.netflix.suro.message;

import com.netflix.suro.message.serde.SerDe;
import com.netflix.suro.message.serde.StringSerDe;
import com.netflix.suro.thrift.TMessageSet;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMessageSet {
    public static List<Message> createMessageList(List<String> routingKeyList, List<String> payloadList) {
        List<Message> messageList = new LinkedList<Message>();

        for (int i = 0; i < routingKeyList.size(); ++i) {
            Message message = new Message(
                    routingKeyList.get(i),
                    payloadList.get(i).getBytes());
            messageList.add(message);
        }

        return messageList;
    }

    private List<String> payloadList;
    private List<String> routingKeyList;
    private List<Message> messageList;

    @Before
    public void generateMessages() {
        payloadList = new LinkedList<String>();
        routingKeyList = new LinkedList<String>();
        for (int i = 0; i < 10; ++i) {
            routingKeyList.add("routingKey" + i);
            payloadList.add("payload" + i);
        }

        messageList = createMessageList(routingKeyList, payloadList);
    }

    @Test
    public void testByteSize() {
        int size = 0;
        for (int i = 0; i < payloadList.size(); ++i) {
            size += 4 + routingKeyList.get(i).length() + 4 + payloadList.get(i).length();
        }

        assertEquals(size, MessageSetBuilder.getByteSize(messageList));

        byte[] buffer = MessageSetBuilder.createPayload(messageList, Compression.NO);
        assertEquals(buffer.length, size);
    }

    @Test
    public void testEmptyBuilder() {
        TMessageSet messageSet = new MessageSetBuilder().build();
        assertEquals(messageSet.getMessages().length, 0);
    }
    @Test
    public void testBuilder() {
        TMessageSet messageSet = buildMessageSet();

        assertEquals(messageSet.getApp(), "app");
        assertEquals(messageSet.getHostname(), "testhost");
        assertEquals(messageSet.getCompression(), 0);
        assertEquals(messageSet.getSerde(), StringSerDe.class.getCanonicalName());
        byte[] bytePayload = messageSet.getMessages();
        byte[] payload = MessageSetBuilder.createPayload(messageList, Compression.NO);
        assertEquals(bytePayload.length, payload.length);
        for (int i = 0; i < bytePayload.length; ++i) {
            assertEquals(bytePayload[i], payload[i]);
        }
    }

    private TMessageSet buildMessageSet() {
        MessageSetBuilder builder = new MessageSetBuilder();
        builder = builder.withApp("app").withHostname("testhost");
        for (Message message : messageList) {
            builder.withMessage(message.getRoutingKey(), message.getPayload());
        }

        return builder.build();
    }

    @Test
    public void testReader() throws Exception {
        TMessageSet messageSet = buildMessageSet();

        MessageSetReader reader = new MessageSetReader(messageSet);
        assertTrue(reader.checkCRC());
        assertEquals(reader.getApp(), "app");
        assertEquals(reader.getHostname(), "testhost");

        SerDe<String> serde = reader.getSerDe();

        int i = 0;
        for (Message message : reader) {
            assertEquals(message.getRoutingKey(), "routingKey" + i);
            assertEquals(message.getApp(), "app");
            assertEquals(message.getHostname(), "testhost");
            assertEquals(serde.deserialize(message.getPayload()), "payload" + i);
            ++i;
        }
        assertEquals(i, 10);
    }

}
