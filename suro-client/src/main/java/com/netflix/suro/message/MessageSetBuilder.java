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

import com.google.common.io.ByteArrayDataOutput;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.client.async.Queue4Client;
import com.netflix.suro.thrift.TMessageSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * The payload for Suro's Thrift communication is {@link TMessageSet}, a thrift presentation of
 * a set of messages. This class can be helpful to easily create {@link TMessageSet} instances.
 *
 * @author jbae
 */
public class MessageSetBuilder {
    private static final Logger log = LoggerFactory.getLogger(MessageSetBuilder.class);

    private final ClientConfig config;
    private List<Message> messageList;
    private Compression compression = Compression.LZF;

    /**
     * @param config contains information including application name, etc
     */
    public MessageSetBuilder(ClientConfig config) {
        this.config = config;
        messageList = new ArrayList<Message>();
    }

    public MessageSetBuilder withMessage(String routingKey, byte[] payload) {
        this.messageList.add(new Message(routingKey, payload));
        return this;
    }

    public MessageSetBuilder withCompression(Compression compresson) {
        this.compression = compresson;
        return this;
    }

    public TMessageSet build() {
        try {
            byte[] buffer = createPayload(messageList, compression);
            long crc = getCRC(buffer);


            return new TMessageSet(
                    config.getApp(),
                    messageList.size(),
                    (byte) compression.getId(),
                    crc,
                    ByteBuffer.wrap(buffer));
        } catch (IOException e) {
            log.error("Exception on building TMessageSet: " + e.getMessage(), e);
            return null;
        } finally {
            messageList.clear();
        }
    }

    /**
     * @return number of messages in MessageSet
     */
    public int size() {
        return messageList.size();
    }

    /**
     * Create compressed byte[] from the list of messages. Each message contains
     * byte[] as its message body, so, this method is simply flattening byte[]
     * for all messages in the messageList
     *
     * @param messageList a list of messages for payload
     * @param compression Compression method to be applied to the payload
     * @return A byte array that encodes the build payload
     * @throws IOException
     */
    public static byte[] createPayload(List<Message> messageList, Compression compression) throws IOException {
        ByteArrayDataOutput out = new ByteArrayDataOutputStream(outputStream.get());
        for (Message message : messageList) {
            message.write(out);
        }

        return compression.compress(out.toByteArray());
    }

    private static ThreadLocal<ByteArrayOutputStream> outputStream =
            new ThreadLocal<ByteArrayOutputStream>() {
                @Override
                protected ByteArrayOutputStream initialValue() {
                    return new ByteArrayOutputStream();
                }

                @Override
                public ByteArrayOutputStream get() {
                    ByteArrayOutputStream b = super.get();
                    b.reset();
                    return b;
                }
            };

    /**
     * Compute CRC32 value for byte[]
     *
     * @param buffer all the bytes in the buffer will be used for CRC32 calculation
     *
     * @return a CRC32 value for the given byte array
     */
    public static long getCRC(byte[] buffer) {
        CRC32 crc = new CRC32();
        crc.update(buffer);
        return crc.getValue();
    }

    /**
     * Drains the given number of messages from the givne queue. Instead of calling {@link #withMessage(String, byte[])},
     * we can call this method.
     * This method is reverse one of JDK BlockingQueue.drainTo.
     *
     * @param queue the queue to drain messages from
     * @param size the number of messages to drain from the given queue
     */
    public void drainFrom(Queue4Client queue, int size) {
        queue.drain(size, messageList);
    }
}
