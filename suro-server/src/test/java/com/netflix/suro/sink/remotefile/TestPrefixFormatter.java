package com.netflix.suro.sink.remotefile;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestPrefixFormatter {
    @Test
    public void testStatic() throws IOException {
        String spec = "{\n" +
                "    \"type\": \"static\",\n" +
                "    \"prefix\": \"prefix\"\n" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        RemotePrefixFormatter formatter = mapper.readValue(spec, new TypeReference<RemotePrefixFormatter>(){});
        assertEquals(formatter.get(), "prefix");
    }

    @Test
    public void testDateRegionStack() throws IOException {
        String spec = "{\n" +
                "    \"type\": \"DateRegionStack\",\n" +
                "    \"date\": \"YYYYMMDD\",\n" +
                "    \"region\": \"us-east-1\",\n" +
                "    \"stack\": \"normal\"\n" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        mapper.setInjectableValues(new InjectableValues() {
            @Override
            public Object findInjectableValue(Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance) {
                return null;
            }
        });
        RemotePrefixFormatter formatter = mapper.readValue(spec, new TypeReference<RemotePrefixFormatter>() {});
        DateTimeFormatter format = DateTimeFormat.forPattern("YYYYMMDD");
        String answer = String.format("%s/us-east-1/normal/", format.print(new DateTime()));
        assertEquals(formatter.get(), answer);
    }

    @Test
    public void testInjectedDateRegionStack() throws IOException {
        String spec = "{\n" +
                "    \"type\": \"DateRegionStack\",\n" +
                "    \"date\": \"YYYYMMDD\"\n" +
                "}";
        final Map<String, Object> injectables = Maps.newHashMap();
        injectables.put("region", "eu-west-1");
        injectables.put("stack", "gps");

        ObjectMapper mapper = new ObjectMapper();
        mapper.setInjectableValues(new InjectableValues() {
            @Override
            public Object findInjectableValue(Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance) {
                return injectables.get(valueId);
            }
        });
        RemotePrefixFormatter formatter = mapper.readValue(spec, new TypeReference<RemotePrefixFormatter>() {});
        DateTimeFormatter format = DateTimeFormat.forPattern("YYYYMMDD");
        String answer = String.format("%s/eu-west-1/gps/", format.print(new DateTime()));
        assertEquals(formatter.get(), answer);
    }
}
