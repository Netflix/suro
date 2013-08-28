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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.zip.CRC32;

public class MessageSetBuilder {
    private String hostname;
    private String app = "default";
    private List<Message> messageList;
    private Compression compression = Compression.NO;
    private SerDe serde = new StringSerDe();

    public MessageSetBuilder() {
        messageList = new LinkedList<Message>();
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "UNKNOWN";
        }
    }

    public MessageSetBuilder withHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public MessageSetBuilder withApp(String app) {
        this.app = app;
        return this;
    }

    public MessageSetBuilder withMessage(String routingKey, byte[] payload) {
        this.messageList.add(new Message(routingKey, payload));
        return this;
    }

    public MessageSetBuilder withSerDe(SerDe serde) {
        this.serde = serde;
        return this;
    }

    public MessageSetBuilder withCompression(Compression compresson) {
        this.compression = compresson;
        return this;
    }

    public TMessageSet build() {
        byte[] buffer = createPayload(messageList, compression);
        long crc = getCRC(buffer);

        messageList.clear();

        return new TMessageSet(
                hostname,
                app,
                serde.getClass().getCanonicalName(),
                compression.getId(),
                crc,
                ByteBuffer.wrap(buffer));
    }

    public int size() {
        return messageList.size();
    }

    public static byte[] createPayload(List<Message> messageList, Compression compression) {
        ByteBuffer buffer = ByteBuffer.allocate(getByteSize(messageList));
        for (Message message : messageList) {
            message.writeTo(buffer);
        }
        buffer.rewind();

        return compression.compress(buffer.array());
    }

    public static int getByteSize(List<Message> messageList) {
        int size = 0;
        for (Message message : messageList) {
            size += message.getByteSize();
        }
        return size;
    }

    public static long getCRC(byte[] buffer) {
        CRC32 crc = new CRC32();
        crc.update(buffer);
        return crc.getValue();
    }

    public void drainFrom(BlockingQueue<Message> queue, int size) {
        queue.drainTo(messageList, size);
    }
}
