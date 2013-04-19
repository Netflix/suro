package com.netflix.suro.message;

import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;
import com.ning.compress.lzf.LZFException;

public enum Compression {
    NO((byte)0) {
        byte[] compress(byte[] buffer) {
            return buffer;
        }
        byte[] decompress(byte[] buffer) {
            return buffer;
        }
    },
    LZF((byte)1) {
        byte[] compress(byte[] buffer) {
            return LZFEncoder.encode(buffer);
        }
        byte[] decompress(byte[] buffer) {
            try {
                return LZFDecoder.decode(buffer);
            } catch (LZFException e) {
                throw new RuntimeException(e);
            }
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

    abstract byte[] compress(byte[] buffer);
    abstract byte[] decompress(byte[] buffer);
}
