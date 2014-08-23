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

import com.netflix.suro.ClientConfig;
import com.netflix.suro.thrift.TMessageSet;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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
    public void testEmptyBuilder() {
        TMessageSet messageSet = new MessageSetBuilder(new ClientConfig()).build();
        assertEquals(messageSet.getNumMessages(), 0);
    }
    
    @Test
    public void testBuilder() throws IOException {
        TMessageSet messageSet = buildMessageSet();

        assertEquals(messageSet.getCompression(), 1);
        byte[] bytePayload = messageSet.getMessages();
        byte[] payload = MessageSetBuilder.createPayload(messageList, Compression.LZF);
        assertEquals(bytePayload.length, payload.length);
        for (int i = 0; i < bytePayload.length; ++i) {
            assertEquals(bytePayload[i], payload[i]);
        }
    }

    private TMessageSet buildMessageSet() {
        MessageSetBuilder builder = new MessageSetBuilder(new ClientConfig());
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

        SerDe<String> serde = new StringSerDe();

        int i = 0;
        for (Message message : reader) {
            assertEquals(message.getRoutingKey(), "routingKey" + i);
            assertEquals(serde.deserialize(message.getPayload()), "payload" + i);
            ++i;
        }
        assertEquals(i, 10);
    }

    @Test
    public void testReaderException() throws IOException {
        MessageSetBuilder builder = new MessageSetBuilder(new ClientConfig()).withCompression(Compression.NO);
        for (Message message : messageList) {
            builder.withMessage(message.getRoutingKey(), message.getPayload());
        }
        TMessageSet messageSet = builder.build();
        byte[] b = messageSet.getMessages();
        // corrup the data
        b[0] = (byte) 0xff;
        b[1] = (byte) 0xff;
        b[2] = (byte) 0xff;
        b[3] = (byte) 0xff;

        MessageSetReader reader = new MessageSetReader(messageSet);
        //assertTrue(reader.checkCRC());

        SerDe<String> serde = new StringSerDe();

        int i = 0;
        for (Message message : reader) {
            assertNull(message);
        }
        assertEquals(i, 0);
    }

}
