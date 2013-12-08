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
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.SerDe;
import org.apache.log4j.Logger;

/**
 * @param <T>
 */
public class JsonSerDe<T> implements SerDe<T> {
    private static final Logger log = Logger.getLogger(JsonSerDe.class);

    private final ObjectMapper jsonMapper = new DefaultObjectMapper();
    private final TypeReference<T> typeReference = new TypeReference<T>(){};

    @Override
    public T deserialize(byte[] payload) {
        try {
            return jsonMapper.readValue(payload, typeReference);
        } catch (Exception e) {
            log.error("deserialize error in JsonSerDe: " + e.getMessage(), e);
            return null;
        }
    }

    @Override
    public byte[] serialize(T payload) {
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
