package com.netflix.suro.message;

public interface SerDe<T> {
    byte getId();

    T deserialize(byte[] payload);

    byte[] serialize(T payload);
}
