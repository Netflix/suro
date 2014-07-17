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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import com.netflix.suro.TypeHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Set;

/**
 * The default {@link ObjectMapper} used for serializing and de-serializing JSON objects.
 */
@Singleton
public class DefaultObjectMapper extends ObjectMapper {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultObjectMapper.class);

    public DefaultObjectMapper() {
        this(null, null);
    }
    
    @Inject
    public DefaultObjectMapper(final Injector injector, Set<TypeHolder> crossInjectable)
    {
        SimpleModule serializerModule = new SimpleModule("SuroServer default serializers");
        serializerModule.addSerializer(ByteOrder.class, ToStringSerializer.instance);
        serializerModule.addDeserializer(
                ByteOrder.class,
                new JsonDeserializer<ByteOrder>()
                {
                    @Override
                    public ByteOrder deserialize(
                            JsonParser jp, DeserializationContext ctxt
                    ) throws IOException, JsonProcessingException
                    {
                        if (ByteOrder.BIG_ENDIAN.toString().equals(jp.getText())) {
                            return ByteOrder.BIG_ENDIAN;
                        }
                        return ByteOrder.LITTLE_ENDIAN;
                    }
                }
        );
        registerModule(serializerModule);
        registerModule(new GuavaModule());

        if (injector != null) {
            setInjectableValues(new InjectableValues() {
                @Override
                public Object findInjectableValue(
                        Object valueId, 
                        DeserializationContext ctxt, 
                        BeanProperty forProperty, 
                        Object beanInstance
                ) {
                    LOG.info("Looking for " + valueId);
                    try {
                        return injector.getInstance(Key.get(forProperty.getType().getRawClass(), Names.named((String)valueId)));
                    } catch (Exception e) {
                        try {
                            return injector.getInstance(forProperty.getType().getRawClass());
                        } catch (Exception ex) {
                            LOG.info("No implementation found, returning null");
                        }
                        return null;
                    }
                }
            });
        }

        configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        configure(MapperFeature.AUTO_DETECT_GETTERS, false);
        configure(MapperFeature.AUTO_DETECT_CREATORS, false);
        configure(MapperFeature.AUTO_DETECT_FIELDS, false);
        configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
        configure(MapperFeature.AUTO_DETECT_SETTERS, false);
        configure(SerializationFeature.INDENT_OUTPUT, false);
        
        if (crossInjectable != null) {
            for (TypeHolder entry : crossInjectable) {
                LOG.info("Registering subtype : " + entry.getName() + " -> " + entry.getRawType().getCanonicalName());
                registerSubtypes(new NamedType(entry.getRawType(), entry.getName()));
            }
        }
    }
}