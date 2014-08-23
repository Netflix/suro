package com.netflix.suro.sink.tranquility;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.sink.Sink;
import io.druid.jackson.AggregatorsModule;
import io.druid.jackson.DruidDefaultSerializersModule;
import io.druid.jackson.QueryGranularityModule;
import io.druid.jackson.SegmentsModule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class TestTranquilitySink {
    @ClassRule
    public static ZkExternalResource zk = new ZkExternalResource();

    @Test
    public void testCreateDefaultArgument() throws IOException {
        String desc = "{\n" +
                "    \"type\": \"tranquility\",\n" +
                "    \"dataSource\": \"test_dataSource\",\n" +
                "    \"dimensions\": [\"f1\", \"f2\", \"f3\"],\n" +
                "    \"druid.zk.service.host\": \"%ZK_HOST%\"\n" +
                "}";

        desc = desc.replace("%ZK_HOST%", zk.getConnectionString());

        final ObjectMapper jsonMapper = new DefaultObjectMapper();
        jsonMapper.registerSubtypes(new NamedType(TranquilitySink.class, TranquilitySink.TYPE));
        jsonMapper.setInjectableValues(new InjectableValues() {
            @Override
            public Object findInjectableValue(
                    Object valueId,
                    DeserializationContext ctxt,
                    BeanProperty forProperty,
                    Object beanInstance
            ) {
                if (valueId.equals(ObjectMapper.class.getCanonicalName())) {
                    return jsonMapper;
                } else {
                    return null;
                }
            }
        });

        Sink sink = jsonMapper.readValue(desc, new TypeReference<Sink>(){});
        assertTrue(sink instanceof TranquilitySink);
    }

    @Test
    public void testCreateAllArguments() throws IOException {
        String desc = "{\n" +
                "    \"type\": \"tranquility\",\n" +
                "    \"queue4Sink\": {\n" +
                "        \"type\": \"blocking\",\n" +
                "        \"capacity\": 100\n" +
                "    },\n" +
                "    \"dataSource\": \"test_dataSource\",\n" +
                "    \"discoveryPath\": \"/DRUID/discovery\",\n" +
                "    \"indexServiceName\": \"DRUID-overlord\",\n" +
                "    \"dimensions\": [\"f1\", \"f2\", \"f3\"],\n" +
                "    \"druid.zk.service.host\": \"%ZK_HOST%\",\n" +
                "    \"aggregators\":[\n" +
                "        {\n" +
                "            \"type\": \"count\",\n" +
                "            \"name\": \"count\"\n" +
                "        }\n" +
                "    ],\n" +
                "    \"indexGranularity\": \"second\",\n" +
                "    \"segmentGranularity\": \"six_hour\",\n" +
                "    \"warmingPeriod\": \"PT10m\",\n" +
                "    \"windowPeriod\": \"PT70m\",\n" +
                "    \"partitions\" : 10,\n" +
                "    \"replicants\": 2\n" +
                "}";

        desc = desc.replace("%ZK_HOST%", zk.getConnectionString());

        final ObjectMapper jsonMapper = new DefaultObjectMapper();
        jsonMapper.registerModule(new DruidDefaultSerializersModule());
        jsonMapper.registerModule(new GuavaModule());
        jsonMapper.registerModule(new QueryGranularityModule());
        jsonMapper.registerModule(new AggregatorsModule());
        jsonMapper.registerModule(new SegmentsModule());

        jsonMapper.registerSubtypes(new NamedType(TranquilitySink.class, TranquilitySink.TYPE));
        jsonMapper.setInjectableValues(new InjectableValues() {
            @Override
            public Object findInjectableValue(
                    Object valueId,
                    DeserializationContext ctxt,
                    BeanProperty forProperty,
                    Object beanInstance
            ) {
                if (valueId.equals(ObjectMapper.class.getCanonicalName())) {
                    return jsonMapper;
                } else {
                    return null;
                }
            }
        });

        Sink sink = jsonMapper.readValue(desc, new TypeReference<Sink>(){});
        assertTrue(sink instanceof TranquilitySink);
    }
}
