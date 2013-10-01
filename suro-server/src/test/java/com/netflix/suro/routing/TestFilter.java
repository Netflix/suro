package com.netflix.suro.routing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.suro.sink.SuroPlugin;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.Message;
import com.netflix.suro.sink.TestSinkManager.TestSink;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFilter {
    private static Injector injector = Guice.createInjector(
            new SuroPlugin() {
                @Override
                protected void configure() {
                    this.addSinkType("TestSink", TestSink.class);
                }
            },
            new AbstractModule() {
                @Override
                protected void configure() {
                    bind(ObjectMapper.class).to(DefaultObjectMapper.class);
                }
            }
        );

    @Test
    public void testRegexFilter() throws IOException {
        String desc = "{\n" +
                "  \"type\":\"regex\",\n" +
                "  \"regex\":\"abcd\"\n" +
                "}";

        ObjectMapper jsonMapper = injector.getInstance(ObjectMapper.class);
        Filter filter = jsonMapper.readValue(desc, new TypeReference<Filter>(){});
        assertTrue(filter.doFilter("abcdefg"));
        assertFalse(filter.doFilter("zcb"));

        assertTrue(filter.doFilter(new Message("routingKey", "abcdefg".getBytes())));
        assertFalse(filter.doFilter(new Message("routingKey", "zcb".getBytes())));
    }
}
