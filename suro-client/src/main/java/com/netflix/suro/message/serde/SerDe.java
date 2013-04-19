package com.netflix.suro.message.serde;

public interface SerDe<T> {
    byte getId();

    T deserialize(byte[] payload);

    byte[] serialize(T payload);

    String toString(byte[] payload);
}
