package com.netflix.suro.queue;

import com.netflix.suro.connection.TestConnectionPool;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageSetReader;
import com.netflix.suro.thrift.TMessageSet;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMessageSetSerDe {
    @Test
    public void test() {
        TMessageSet messageSet = TestConnectionPool.createMessageSet(100);
        MessageSetSerDe serde = new MessageSetSerDe();
        byte[] payload = serde.serialize(messageSet);
        TMessageSet d = serde.deserialize(payload);

        assertEquals(d.getHostname(), messageSet.getHostname());
        assertEquals(d.getApp(), messageSet.getApp());
        assertEquals(d.getDataType(), messageSet.getDataType());
        assertTrue(Arrays.equals(d.getMessages(), messageSet.getMessages()));

        List<Message> messageList = new LinkedList<Message>();
        for (Message m : new MessageSetReader(messageSet)) {
            messageList.add(m);
        }
        List<Message> dMessasgeList = new LinkedList<Message>();
        for (Message m : new MessageSetReader(d)) {
            dMessasgeList.add(m);
        }
        assertEquals(messageList, dMessasgeList);
    }
}
