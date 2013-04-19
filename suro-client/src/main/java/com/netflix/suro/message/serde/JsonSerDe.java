package com.netflix.suro.message.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;

import java.util.Map;

public class JsonSerDe implements SerDe<Map<String, Object>> {
    static Logger log = Logger.getLogger(JsonSerDe.class);

    public static final byte id = 1;
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final TypeReference<Map<String, Object>> typeReference = new TypeReference<Map<String, Object>>(){};
    @Override
    public byte getId() {
        return id;
    }

    @Override
    public Map<String, Object> deserialize(byte[] payload) {
        try {
            return jsonMapper.readValue(payload, typeReference);
        } catch (Exception e) {
            log.error("deserialize error in JsonSerDe: " + e.getMessage(), e);
            return Maps.newHashMap();
        }
    }

    @Override
    public byte[] serialize(Map<String, Object> payload) {
        try {
            return jsonMapper.writeValueAsBytes(payload);
        } catch (Exception e) {
            log.error("serialize error in JsonSerDe: " + e.getMessage(), e);
            return new byte[]{};
        }
    }

    @Override
    public String toString(byte[] payload) {
        return new String(payload); // json payload is string
    }
}
