package com.netflix.suro.message;

public class DefaultSerDeFactory implements SerDeFactory {
    public static SerDe create(byte id) {
        if (id == StringSerDe.id()) {
            return new StringSerDe();
        } else {
            throw new IllegalArgumentException("invalid SerDe id: " + id);
        }
    }
}
