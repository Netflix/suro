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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Suro message payload contains routing key as String and payload as byte[].
 * This implements Hadoop Writable interface, so it can be written to Hadoop
 * Sequence file. Empty constructor is needed for Writable interface.
 *
 * @author jbae
 */
public class Message implements Writable {
    private String routingKey;
    private byte[] payload;

    public Message() {}
    public Message(String routingKey, byte[] payload) {
        this.routingKey = routingKey;
        this.payload = payload;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public byte[] getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return String.format("routingKey: %s, hostname: %s, app: %s, serde: %s, payload byte size: %d",
                routingKey,
                payload.length);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (o instanceof Message) == false) {
            return false;
        }

        Message m = (Message) o;
        return routingKey.equals(m.routingKey) &&
               Arrays.equals(payload, m.payload);
    }

    @Override
    public int hashCode() {
        int routingKeyHash = routingKey.hashCode();
        int messageHash = payload.hashCode();

        return routingKeyHash + messageHash;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(routingKey);
        dataOutput.writeInt(payload.length);
        dataOutput.write(payload);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        routingKey = dataInput.readUTF();
        payload = new byte[dataInput.readInt()];
        dataInput.readFully(payload);
    }
}
