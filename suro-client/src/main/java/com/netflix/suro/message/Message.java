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
import com.netflix.suro.message.serde.SerDeFactory;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Message implements Writable {
    private String routingKey;
    private String app;
    private String hostname;
    private SerDe serde;
    private byte[] payload;

    public Message() {}
    // constructor for MessageSetBuilder
    public Message(String routingKey, byte[] payload) {
        this.routingKey = routingKey;
        this.app = null;
        this.hostname = null;
        this.serde = null;
        this.payload = payload;
    }

    // constructor for MessageSetReader
    public Message(String routingKey,
                   String app,
                   String hostname,
                   SerDe serde,
                   byte[] payload) {
        this.routingKey = routingKey;
        this.app = app;
        this.hostname = hostname;
        this.serde = serde;
        this.payload = payload;
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(routingKey.length());
        buffer.put(routingKey.getBytes());
        //payload_len payload
        buffer.putInt(payload.length);
        buffer.put(payload);
    }

    public static Message createFrom(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte[] routingKeyBytes = new byte[buffer.getInt()];
        buffer.get(routingKeyBytes);
        byte[] payloadBytes = new byte[buffer.getInt()];
        buffer.get(payloadBytes);
        return new Message(
                new String(routingKeyBytes),
                payloadBytes);
    }

    public int getByteSize() {
        return 4 + routingKey.length() + 4 + payload.length;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public String getHostname() {
        return hostname;
    }

    public String getApp() {
        return app;
    }

    public SerDe getSerDe() {
        return serde;
    }

    public byte[] getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return String.format("routingKey: %s, byte size: %d", routingKey, getByteSize());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (o instanceof Message) == false) {
            return false;
        }

        Message m = (Message) o;
        return (hostname == null ? m.hostname == null : hostname.equals(m.hostname)) &&
               (app == null ? m.app == null : app.equals(m.app)) &&
               routingKey.equals(m.routingKey) &&
               Arrays.equals(payload, m.payload);
    }

    @Override
    public int hashCode() {
        int hostnameHash = hostname == null ? 0 : hostname.hashCode();
        int appHash = app == null ? 0 : app.hashCode();
        int routingKeyHash = routingKey.hashCode();
        int messageHash = payload.hashCode();

        return hostnameHash + appHash + routingKeyHash + messageHash;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(hostname);
        dataOutput.writeUTF(app);
        dataOutput.writeUTF(routingKey);
        dataOutput.writeUTF(serde.getClass().getName());
        dataOutput.writeInt(payload.length);
        dataOutput.write(payload);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        hostname = dataInput.readUTF();
        app = dataInput.readUTF();
        routingKey = dataInput.readUTF();
        serde = SerDeFactory.create(dataInput.readUTF());
        payload = new byte[dataInput.readInt()];
        dataInput.readFully(payload);
    }
}
