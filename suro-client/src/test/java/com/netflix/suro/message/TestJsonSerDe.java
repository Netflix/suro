package com.netflix.suro.message;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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

        ObjectMapper jsonMapper = new ObjectMapper();
        TypeReference<Map<String, Object>> type = new TypeReference<Map<String, Object>>(){};
        Map<String, Object> map = jsonMapper.readValue(obj, type);
        assertEquals(map.get("field2"), 3); // just check whether the object is created properly

        JsonSerDe serde = new JsonSerDe();
        byte[] bytes = serde.serialize(map);
        Map<String, Object> map2 = jsonMapper.readValue(bytes, type);
        assertEquals(map, map2);

        map2 = serde.deserialize(bytes);
        assertEquals(map, map2);

        assertEquals(obj.replaceAll("[\\t\\r\\n ]", ""), serde.toString(bytes));
    }
}
