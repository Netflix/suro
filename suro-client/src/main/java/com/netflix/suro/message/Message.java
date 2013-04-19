package com.netflix.suro.message;

import java.nio.ByteBuffer;

public class Message {
    private final String routingKey;
    private final String app;
    private final String hostname;
    private final String dataType;
    private final byte[] payload;

    // constructor for MessageSetBuilder
    public Message(String routingKey, byte[] payload) {
        this.routingKey = routingKey;
        this.app = null;
        this.hostname = null;
        this.dataType = null;
        this.payload = payload;
    }

    // constructor for MessageSetReader
    Message(String routingKey,
                   String app,
                   String hostname,
                   String dataType,
                   byte[] payload) {
        this.routingKey = routingKey;
        this.app = app;
        this.hostname = hostname;
        this.dataType = dataType;
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

    public String getDataType() {
        return dataType;
    }

    public byte[] getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return String.format("routingKey: %s, byte size: %d", routingKey, getByteSize());
    }
}
