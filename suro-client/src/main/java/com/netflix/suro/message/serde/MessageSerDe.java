package com.netflix.suro.message.serde;

import com.netflix.suro.message.Message;

import java.nio.ByteBuffer;

public class MessageSerDe implements SerDe<Message> {
    @Override
    public byte getId() {
        return 100;
    }

    @Override
    public Message deserialize(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);

        byte[] routingKeyBytes = new byte[buffer.getInt()];
        buffer.get(routingKeyBytes);

        byte[] payloadBytes = new byte[buffer.getInt()];
        buffer.get(payloadBytes);

        return new Message(new String(routingKeyBytes), payloadBytes);
    }

    @Override
    public byte[] serialize(Message payload) {
        ByteBuffer buffer = ByteBuffer.allocate(payload.getByteSize());
        payload.writeTo(buffer);
        return buffer.array();
    }

    @Override
    public String toString(byte[] payload) {
        return deserialize(payload).toString();
    }
}
