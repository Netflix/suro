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

package com.netflix.suro.jackson;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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
        String spec = "{\"a\":\"aaa\", \"b\":\"bbb\"}";

        ObjectMapper mapper = new DefaultObjectMapper();
        final Map<String, Object> injectables = Maps.newHashMap();

        injectables.put("test", "test");
        injectables.put("b", "binjected");
        mapper.setInjectableValues(new InjectableValues() {
            @Override
            public Object findInjectableValue(
                    Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance
            ) {
                return injectables.get(valueId);
            }
        });

        TestClass test = mapper.readValue(spec, new TypeReference<TestClass>(){});
        assertEquals(test.getTest(), "test");
        assertEquals(test.getB(), "bbb");
    }

    public static class TestClass {
        @JacksonInject("test")
        private String test;

        private String a;
        private String b;

        @JsonCreator
        public TestClass(
                @JsonProperty("a") String a,
                @JsonProperty("b") @JacksonInject("b") String b) {
            this.a = a;
            this.b = b;
        }
        public String getTest() { return test; }

        public String getA() { return a; }
        public String getB() { return b; }
    }
}
