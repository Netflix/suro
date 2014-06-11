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

import com.netflix.suro.message.SerDe;
import com.netflix.suro.thrift.TMessageSet;
import com.netflix.suro.util.Closeables;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import java.nio.ByteBuffer;

/**
 * {@link SerDe} implementation that serializes and de-serializes {@link TMessageSet}. This is serde is used to persist
 * {@link TMessageSet} objects to a disk-based queue. Messages in {@link TMessageSet} objects that are not consumed would be lost when the server fails,
 * if only in-memory queue is used to store {@link TMessageSet} objects.
 *
 * @author jbae
 */
public class MessageSetSerDe implements SerDe<TMessageSet> {

    @Override
    public TMessageSet deserialize(byte[] payload) {
        DataInputBuffer inBuffer = new DataInputBuffer();
        inBuffer.reset(payload, payload.length);

        try {
            String app = inBuffer.readUTF();
            int numMessages = inBuffer.readInt();
            byte compression = inBuffer.readByte();
            long crc = inBuffer.readLong();
            byte[] messages = new byte[inBuffer.readInt()];
            inBuffer.read(messages);

            return new TMessageSet(
                app,
                numMessages,
                compression,
                crc,
                ByteBuffer.wrap(messages)
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to de-serialize payload into TMessageSet: "+e.getMessage(), e);
        } finally {
            Closeables.closeQuietly(inBuffer);
        }
    }

    @Override
    public byte[] serialize(TMessageSet payload) {
        DataOutputBuffer outBuffer = new DataOutputBuffer();

        try {
            outBuffer.reset();

            outBuffer.writeUTF(payload.getApp());
            outBuffer.writeInt(payload.getNumMessages());
            outBuffer.writeByte(payload.getCompression());
            outBuffer.writeLong(payload.getCrc());
            outBuffer.writeInt(payload.getMessages().length);
            outBuffer.write(payload.getMessages());

            return ByteBuffer.wrap(outBuffer.getData(), 0, outBuffer.getLength()).array();
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize TMessageSet: "+e.getMessage(), e);
        } finally {
            Closeables.closeQuietly(outBuffer);
        }
    }

    @Override
    public String toString(byte[] payload) {
        return deserialize(payload).toString();
    }
}
