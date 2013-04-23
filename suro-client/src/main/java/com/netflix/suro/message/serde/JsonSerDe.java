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

package com.netflix.suro.message.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.netflix.suro.jackson.DefaultObjectMapper;
import org.apache.log4j.Logger;

import java.util.Map;

public class JsonSerDe implements SerDe<Map<String, Object>> {
    static Logger log = Logger.getLogger(JsonSerDe.class);

    public static final byte id = 1;
    private final ObjectMapper jsonMapper = new DefaultObjectMapper();
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
