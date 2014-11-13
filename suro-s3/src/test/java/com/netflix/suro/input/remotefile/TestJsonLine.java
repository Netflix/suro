package com.netflix.suro.input.remotefile;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.MessageContainer;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestJsonLine {
    @Test
    public void shouldReturnStaticRoutingKey() throws Exception {
        ObjectMapper jsonMapper = new DefaultObjectMapper();

        JsonLine jsonLine = new JsonLine(
                "staticRoutingKey",
                null,
                new DefaultObjectMapper());

        Map<String, Object> msgMap = new ImmutableMap.Builder<String, Object>().put("f1", "v1").put("f2", "v2").build();
        List<MessageContainer> messages = jsonLine.parse(jsonMapper.writeValueAsString(msgMap));
        assertEquals(messages.size(), 1);
        assertEquals(messages.get(0).getRoutingKey(), "staticRoutingKey");
        assertEquals(messages.get(0).getEntity(S3Consumer.typeReference), msgMap);
    }

    @Test
    public void shouldReturnRoutingKeyField() throws Exception {
        ObjectMapper jsonMapper = new DefaultObjectMapper();

        JsonLine jsonLine = new JsonLine(
                null,
                "f1",
                new DefaultObjectMapper());

        Map<String, Object> msgMap = new ImmutableMap.Builder<String, Object>().put("f1", "v1").put("f2", "v2").build();
        List<MessageContainer> messages = jsonLine.parse(jsonMapper.writeValueAsString(msgMap));
        assertEquals(messages.size(), 1);
        assertEquals(messages.get(0).getRoutingKey(), "v1");
        assertEquals(messages.get(0).getEntity(S3Consumer.typeReference), msgMap);
    }

    @Test
    public void shouldReturnStaticRoutingKeyOnNonExistingRoutingKeyField() throws Exception {
        ObjectMapper jsonMapper = new DefaultObjectMapper();

        JsonLine jsonLine = new JsonLine(
                "defaultRoutingKey",
                "f1",
                new DefaultObjectMapper());

        Map<String, Object> msgMap = new ImmutableMap.Builder<String, Object>().put("f3", "v3").put("f2", "v2").build();
        List<MessageContainer> messages = jsonLine.parse(jsonMapper.writeValueAsString(msgMap));
        assertEquals(messages.size(), 1);
        assertEquals(messages.get(0).getRoutingKey(), "defaultRoutingKey");
        assertEquals(messages.get(0).getEntity(S3Consumer.typeReference), msgMap);
    }

    @Test
    public void testWithNonParseableMessage() throws Exception {
        JsonLine jsonLine = new JsonLine(
                "defaultRoutingKey",
                "f1",
                new DefaultObjectMapper());

        List<MessageContainer> messages = jsonLine.parse("non_parseable_msg");
        assertEquals(messages.size(), 1);
        assertEquals(messages.get(0).getRoutingKey(), "defaultRoutingKey");
        try {
            messages.get(0).getEntity(S3Consumer.typeReference);
            assertEquals(messages.get(0).getEntity(String.class), "non_parseable_msg");
            fail("exception should be thrown");
        } catch (Exception e) {}

        jsonLine = new JsonLine(
                null,
                "f1",
                new DefaultObjectMapper());
        assertEquals(jsonLine.parse("non_parseable_msg").size(), 0);
    }
}
