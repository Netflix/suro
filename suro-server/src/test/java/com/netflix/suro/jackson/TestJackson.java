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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class TestJackson {
    @Test
    public void test() throws IOException {
        String spec = "{\"type\":\"test1\", \"a\":\"aaa\", \"b\":\"bbb\"}";

        ObjectMapper mapper = new DefaultObjectMapper(null);
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
    
    @Test
    public void testWithGuice() throws IOException {
        final Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(String.class)
                .annotatedWith(Names.named("b"))
                .toInstance("$b$");
                
                bind(String.class)
                .annotatedWith(Names.named("test"))
                .toInstance("$test$");
            }
        });
        
        String spec = "{\"type\":\"test1\", \"a\":\"aaa\", \"b\":\"bbb\"}";

        ObjectMapper mapper = new DefaultObjectMapper(injector);
        final Map<String, Object> injectables = Maps.newHashMap();

        TestClass test = mapper.readValue(spec, TestClass.class);
        assertEquals("$test$", test.getTest());
    }
}