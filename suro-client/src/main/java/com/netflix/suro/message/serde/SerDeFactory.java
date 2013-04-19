package com.netflix.suro.message.serde;

public interface SerDeFactory {
    SerDe create(byte id);
}
