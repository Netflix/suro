package com.netflix.suro.message;

public class StringSerDe implements SerDe<String> {
    public static byte id() {
        return 0;
    }

    @Override
    public byte getId() {
        return 0;
    }

    @Override
    public String deserialize(byte[] message) {
        return new String(message);
    }

    @Override
    public byte[] serialize(String content) {
        return content.getBytes();
    }
}
