package com.netflix.suro.sink.tranquility;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.suro.jackson.DefaultObjectMapper;
import io.druid.jackson.AggregatorsModule;
import io.druid.jackson.DruidDefaultSerializersModule;
import io.druid.jackson.QueryGranularityModule;
import io.druid.jackson.SegmentsModule;

public class ObjectMapperProvider implements Provider<ObjectMapper> {
    private final ObjectMapper jsonMapper;

    @Inject
    public ObjectMapperProvider(DefaultObjectMapper jsonMapper) {
        this.jsonMapper = jsonMapper;

        this.jsonMapper.registerModule(new DruidDefaultSerializersModule());
        this.jsonMapper.registerModule(new GuavaModule());
        this.jsonMapper.registerModule(new QueryGranularityModule());
        this.jsonMapper.registerModule(new AggregatorsModule());
        this.jsonMapper.registerModule(new SegmentsModule());
    }

    @Override
    public ObjectMapper get() {
        return jsonMapper;
    }
}
