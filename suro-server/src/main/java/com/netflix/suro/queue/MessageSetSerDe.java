package com.netflix.suro.queue;

import com.netflix.suro.message.serde.SerDe;
import com.netflix.suro.thrift.TMessageSet;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;

import java.nio.ByteBuffer;

public class MessageSetSerDe implements SerDe<TMessageSet> {
    private DataOutputBuffer outBuffer = new DataOutputBuffer();
    private DataInputBuffer inBuffer = new DataInputBuffer();

    @Override
    public byte getId() {
        return 101;
    }

    @Override
    public TMessageSet deserialize(byte[] payload) {
        inBuffer.reset(payload, payload.length);

        try {
            String hostname = WritableUtils.readString(inBuffer);
            String app = WritableUtils.readString(inBuffer);
            String dataType = WritableUtils.readString(inBuffer);
            byte serde = inBuffer.readByte();
            byte compression = inBuffer.readByte();
            long crc = inBuffer.readLong();
            byte[] messages = new byte[inBuffer.readInt()];
            inBuffer.read(messages);

            return new TMessageSet(
                    hostname,
                    app,
                    dataType,
                    serde,
                    compression,
                    crc,
                    ByteBuffer.wrap(messages)
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] serialize(TMessageSet payload) {
        try {
            outBuffer.reset();

            WritableUtils.writeString(outBuffer, payload.getHostname());
            WritableUtils.writeString(outBuffer, payload.getApp());
            WritableUtils.writeString(outBuffer, payload.getDataType());
            outBuffer.writeByte(payload.getSerde());
            outBuffer.writeByte(payload.getCompression());
            outBuffer.writeLong(payload.getCrc());
            outBuffer.writeInt(payload.getMessages().length);
            outBuffer.write(payload.getMessages());

            return ByteBuffer.wrap(outBuffer.getData(), 0, outBuffer.getLength()).array();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString(byte[] payload) {
        return deserialize(payload).toString();
    }
}
