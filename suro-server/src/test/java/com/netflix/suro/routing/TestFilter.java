package com.netflix.suro.routing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.Message;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFilter {
    private ObjectMapper jsonMapper = new DefaultObjectMapper();

    @Test
    public void testRegexFilter() throws IOException {
        String desc = "{\n" +
                "  \"type\":\"regex\",\n" +
                "  \"regex\":\"abcd\"\n" +
                "}";

        Filter filter = jsonMapper.readValue(desc, new TypeReference<Filter>(){});
        assertTrue(filter.doFilter("abcdefg"));
        assertFalse(filter.doFilter("zcb"));

        assertTrue(filter.doFilter(new Message("routingKey", "abcdefg".getBytes())));
        assertFalse(filter.doFilter(new Message("routingKey", "zcb".getBytes())));
    }
}
