package com.netflix.suro;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.inject.Provider;
import com.netflix.suro.jackson.DefaultObjectMapper;
import io.druid.jackson.AggregatorsModule;
import io.druid.jackson.DruidDefaultSerializersModule;
import io.druid.jackson.QueryGranularityModule;
import io.druid.jackson.SegmentsModule;

public class ObjectMapperProvider implements Provider<ObjectMapper> {
    private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

    static {
        jsonMapper.registerModule(new DruidDefaultSerializersModule());
        jsonMapper.registerModule(new GuavaModule());
        jsonMapper.registerModule(new QueryGranularityModule());
        jsonMapper.registerModule(new AggregatorsModule());
        jsonMapper.registerModule(new SegmentsModule());
    }

    @Override
    public ObjectMapper get() {
        return jsonMapper;
    }
}
