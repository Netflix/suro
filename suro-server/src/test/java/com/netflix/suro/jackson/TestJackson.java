package com.netflix.suro.jackson;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class TestJackson {
    @Test
    public void test() throws IOException {
        String spec = "{\"type\":\"test1\", \"a\":\"aaa\", \"b\":\"bbb\"}";

        ObjectMapper mapper = new DefaultObjectMapper();
        final Map<String, Object> injectables = Maps.newHashMap();

        injectables.put("test", "test");
        mapper.setInjectableValues(new InjectableValues() {
            @Override
            public Object findInjectableValue(
                    Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance
            ) {
                return injectables.get(valueId);
            }
        });

        TestInterface test = mapper.readValue(spec, new TypeReference<TestInterface>(){});
        assertEquals(test.getTest(), "test");
    }
}