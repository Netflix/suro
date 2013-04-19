package com.netflix.suro.message;

import com.netflix.suro.message.serde.DefaultSerDeFactory;
import com.netflix.suro.message.serde.SerDe;
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
        assertEquals(messageSet.getDataType(), "string");
        assertEquals(messageSet.getHostname(), "testhost");
        assertEquals(messageSet.getCompression(), 0);
        assertEquals(messageSet.getSerde(), 0);
        byte[] bytePayload = messageSet.getMessages();
        byte[] payload = MessageSetBuilder.createPayload(messageList, Compression.NO);
        assertEquals(bytePayload.length, payload.length);
        for (int i = 0; i < bytePayload.length; ++i) {
            assertEquals(bytePayload[i], payload[i]);
        }
    }

    private TMessageSet buildMessageSet() {
        MessageSetBuilder builder = new MessageSetBuilder();
        builder = builder.withApp("app").withDatatype("string").withHostname("testhost");
        for (Message message : messageList) {
            builder.withMessage(message.getRoutingKey(), message.getPayload());
        }

        return builder.build();
    }

    @Test
    public void testReader() {
        TMessageSet messageSet = buildMessageSet();

        MessageSetReader reader = new MessageSetReader(messageSet);
        assertTrue(reader.checkCRC());
        assertEquals(reader.getApp(), "app");
        assertEquals(reader.getDataType(), "string");
        assertEquals(reader.getHostname(), "testhost");

        SerDe<String> serde = new DefaultSerDeFactory().create(reader.getSerDeId());

        int i = 0;
        for (Message message : reader) {
            assertEquals(message.getRoutingKey(), "routingKey" + i);
            assertEquals(message.getApp(), "app");
            assertEquals(message.getHostname(), "testhost");
            assertEquals(message.getDataType(), "string");
            assertEquals(serde.deserialize(message.getPayload()), "payload" + i);
            ++i;
        }
        assertEquals(i, 10);
    }

}
