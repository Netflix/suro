package com.netflix.suro.message;

import java.nio.ByteBuffer;

public enum Compression {
    NO((byte)0) {
        ByteBuffer compress(ByteBuffer buffer) {
            return buffer;
        }
        ByteBuffer decompress(ByteBuffer buffer) {
            return buffer;
        }
    };
    private final byte id;
    Compression(byte id) { this.id = id; }

    public byte getId() { return id; }

    public static Compression create(byte id) {
        for (Compression compression : values()) {
            if (id == compression.getId()) {
                return compression;
            }
        }

        throw new IllegalArgumentException("invalid compression id: " + id);
    }

    abstract ByteBuffer compress(ByteBuffer buffer);
    abstract ByteBuffer decompress(ByteBuffer buffer);
}
