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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.serde.JsonSerDe;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestJsonSerDe {
    @Test
    public void test() throws IOException {
        String obj = "{\n" +
                "    \"field1\": \"value1\",\n" +
                "    \"field2\": 3,\n" +
                "    \"field3\": {\n" +
                "        \"field3_1\": \"value3_1\",\n" +
                "        \"field3_2\": \"value3_2\"\n" +
                "    }\n" +
                "}";

        ObjectMapper jsonMapper = new DefaultObjectMapper();
        TypeReference<Map<String, Object>> type = new TypeReference<Map<String, Object>>(){};
        Map<String, Object> map = jsonMapper.readValue(obj, type);
        assertEquals(map.get("field2"), 3); // just check whether the object is created properly

        JsonSerDe<Map<String, Object>> serde = new JsonSerDe<Map<String, Object>>();
        byte[] bytes = serde.serialize(map);
        Map<String, Object> map2 = jsonMapper.readValue(bytes, type);
        assertEquals(map, map2);

        map2 = serde.deserialize(bytes);
        assertEquals(map, map2);

        assertEquals(obj.replaceAll("[\\t\\r\\n ]", ""), serde.toString(bytes));
    }
}
