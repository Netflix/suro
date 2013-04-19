package com.netflix.suro.message.serde;

import com.google.inject.Singleton;

@Singleton
public class DefaultSerDeFactory implements SerDeFactory {

    @Override
    public SerDe create(byte id) {
        switch (id) {
            case StringSerDe.id:
                return new StringSerDe();
            case JsonSerDe.id:
                return new JsonSerDe();
            default:
                throw new IllegalArgumentException("invalid SerDe id: " + id);
        }
    }
}
