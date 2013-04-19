package com.netflix.suro.message.serde;

public class StringSerDe implements SerDe<String> {
    public static final byte id = 0;

    @Override
    public byte getId() {
        return id;
    }

    @Override
    public String deserialize(byte[] message) {
        return new String(message);
    }

    @Override
    public byte[] serialize(String content) {
        return content.getBytes();
    }

    @Override
    public String toString(byte[] message) {
        return new String(message);
    }
}
