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

package com.netflix.suro.input.thrift;

import com.netflix.suro.connection.TestConnectionPool;
import com.netflix.suro.input.thrift.MessageSetSerDe;
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
