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

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import com.netflix.suro.thrift.TMessageSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Implements Iterator for each message in TMessageSet
 *
 * @author jbae
 */
public class MessageSetReader implements Iterable<Message> {
    static Logger log = LoggerFactory.getLogger(MessageSetReader.class);

    private final TMessageSet messageSet;

    public MessageSetReader(TMessageSet messageSet) {
        this.messageSet = messageSet;
    }

    public boolean checkCRC() {
        long crcReceived = messageSet.getCrc();
        long crc = MessageSetBuilder.getCRC(messageSet.getMessages());

        return crcReceived == crc;
    }

    @Override
    public Iterator<Message> iterator() {
        try {
            final ByteArrayDataInput input = ByteStreams.newDataInput(
                    Compression.create(messageSet.getCompression()).decompress(messageSet.getMessages()));

            return new Iterator<Message>() {
                private int messageCount = messageSet.getNumMessages();

                @Override
                public boolean hasNext() {
                    return messageCount > 0;
                }

                @Override
                public Message next() {
                    Message m = new Message();
                    try {
                        m.readFields(input);
                        --messageCount;
                        return m;
                    } catch (IOException e) {
                        log.error("Exception while iterating MessageSet:" + e.getMessage(), e);
                        return null;
                    }
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("remove is not supported");
                }
            };
        } catch (Exception e) {
            log.error("Exception while reading: " + e.getMessage(), e);
            return new Iterator<Message>() {
                @Override
                public boolean hasNext() {
                    return false;
                }
                @Override
                public Message next() {
                    return null;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("remove is not supported");
                }
            };
        }
    }
}
