package com.netflix.suro.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.datatype.guava.GuavaModule;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 */
public class DefaultObjectMapper extends ObjectMapper
{
    public DefaultObjectMapper()
    {
        this(null);
    }

    public DefaultObjectMapper(JsonFactory factory)
    {
        super(factory);
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

        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        configure(MapperFeature.AUTO_DETECT_GETTERS, false);
        //configure(MapperFeature.AUTO_DETECT_CREATORS, false); https://github.com/FasterXML/jackson-databind/issues/170
        configure(MapperFeature.AUTO_DETECT_FIELDS, false);
        configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
        configure(MapperFeature.AUTO_DETECT_SETTERS, false);
        configure(SerializationFeature.INDENT_OUTPUT, false);
    }
}