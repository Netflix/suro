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

import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;

import java.io.IOException;

/**
 * Suro message payload compression
 *
 * The method {@link #compress(byte[])} receives byte[] and returns compressed byte[]
 * The method {@link #decompress(byte[])} receives compressed byte[] and returns uncompressed one
 *
 * 0, NO no compression
 * 1, LZF LZF compression
 *
 * @author jbae
 */
public enum Compression {
    NO(0) {
        byte[] compress(byte[] buffer) {
            return buffer;
        }
        byte[] decompress(byte[] buffer) {
            return buffer;
        }
    },
    LZF(1) {
        byte[] compress(byte[] buffer) {
            try {
                return LZFEncoder.encode(buffer);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        byte[] decompress(byte[] buffer) {
            try {
                return LZFDecoder.decode(buffer);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };
    private final int id;
    Compression(int id) { this.id = id; }

    public int getId() { return id; }

    public static Compression create(int id) {
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
