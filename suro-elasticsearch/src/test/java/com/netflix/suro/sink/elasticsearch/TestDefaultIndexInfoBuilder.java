package com.netflix.suro.sink.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.Message;
import com.netflix.suro.sink.DataConverter;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestDefaultIndexInfoBuilder {
    private ObjectMapper jsonMapper = new DefaultObjectMapper();

    @Before
    public void setup() {
        System.setProperty("user.timezone", "GMT");
    }

    @Test
    public void shouldReturnNullOnParsingFailure() {
        DefaultIndexInfoBuilder builder = new DefaultIndexInfoBuilder(
                null,
                null,
                null,
                null,
                null,
                jsonMapper);

        assertNull(builder.create(new Message("routingkey", "message".getBytes())));
    }

    @Test
    public void shouldNullOrEmptyIndexTypeMapReturnRoutingKey() throws JsonProcessingException {
        DefaultIndexInfoBuilder builder = new DefaultIndexInfoBuilder(
                null,
                null,
                null,
                null,
                null,
                jsonMapper);

        Map<String, Object> msg = new ImmutableMap.Builder<String, Object>().put("f1", "v1").build();

        IndexInfo info = builder.create(new Message("routingkey", jsonMapper.writeValueAsBytes(msg)));
        assertEquals(info.getIndex(), "routingkey");
        assertEquals(info.getType(), "default");
    }

    @Test
    public void shouldIndexTypeMapReturnSetting() throws JsonProcessingException {
        DefaultIndexInfoBuilder builder = new DefaultIndexInfoBuilder(
                new ImmutableMap.Builder<String, String>()
                        .put("routingkey1", "index1:type1")
                        .put("routingkey2", "index2").build(),
                null,
                null,
                null,
                null,
                jsonMapper);

        Map<String, Object> msg = new ImmutableMap.Builder<String, Object>().put("f1", "v1").build();

        IndexInfo info = builder.create(new Message("routingkey1", jsonMapper.writeValueAsBytes(msg)));
        assertEquals(info.getIndex(), "index1");
        assertEquals(info.getType(), "type1");

        info = builder.create(new Message("routingkey2", jsonMapper.writeValueAsBytes(msg)));
        assertEquals(info.getIndex(), "index2");
        assertEquals(info.getType(), "default");
    }

    @Test
    public void shouldIndexFormatterWorkWithTimestampField() throws JsonProcessingException {
        Properties props = new Properties();
        props.put("dateFormat", "YYYYMMdd");

        DefaultIndexInfoBuilder builder = new DefaultIndexInfoBuilder(
                new ImmutableMap.Builder<String, String>()
                        .put("routingkey1", "index1:type1")
                        .put("routingkey2", "index2").build(),
                null,
                new TimestampField("ts", null),
                new IndexSuffixFormatter("date", props),
                null,
                jsonMapper);

        DateTime dt = new DateTime("2014-10-12T00:00:00.000Z");
        Map<String, Object> msg = new ImmutableMap.Builder<String, Object>()
                .put("ts", dt.getMillis())
                .put("f1", "v1").build();

        IndexInfo info = builder.create(new Message("routingkey1", jsonMapper.writeValueAsBytes(msg)));
        assertEquals(info.getIndex(), "index120141012");
        assertEquals(info.getType(), "type1");

        info = builder.create(new Message("routingkey2", jsonMapper.writeValueAsBytes(msg)));
        assertEquals(info.getIndex(), "index220141012");
        assertEquals(info.getType(), "default");
    }

    @Test
    public void shouldSourceConvertedOrNot() throws IOException {
        DataConverter converter = new DataConverter() {
            @Override
            public Map<String, Object> convert(Map<String, Object> msg) {
                msg.put("app", "app");
                return msg;
            }
        };

        DefaultIndexInfoBuilder builder = new DefaultIndexInfoBuilder(
                null,
                null,
                null,
                null,
                converter,
                jsonMapper);

        Map<String, Object> msg = new ImmutableMap.Builder<String, Object>()
                .put("f1", "v1").build();

        IndexInfo info = builder.create(new Message("routingkey", jsonMapper.writeValueAsBytes(msg)));
        Map<String, Object> source = (Map<String, Object>) info.getSource();
        assertEquals(source.get("app"), "app");
        assertEquals(source.get("f1"), "v1");

        builder = new DefaultIndexInfoBuilder(
                null,
                null,
                null,
                null,
                null,
                jsonMapper);

        msg = new ImmutableMap.Builder<String, Object>()
                .put("f1", "v1").build();

        info = builder.create(new Message("routingkey", jsonMapper.writeValueAsBytes(msg)));
        source = jsonMapper.readValue((String)info.getSource(), new TypeReference<Map<String, Object>>() {});
        assertNull(source.get("app"));
        assertEquals(source.get("f1"), "v1");
    }

    @Test
    public void shouldGetIdReturnNullOnEmptyList() throws JsonProcessingException {
        DefaultIndexInfoBuilder builder = new DefaultIndexInfoBuilder(
                null,
                null,
                null,
                null,
                null,
                jsonMapper);

        Map<String, Object> msg = new ImmutableMap.Builder<String, Object>()
                .put("f1", "v1").build();


        IndexInfo info = builder.create(new Message("routingkey", jsonMapper.writeValueAsBytes(msg)));
        assertNull(info.getId());

        builder = new DefaultIndexInfoBuilder(
                null,
                new ImmutableMap.Builder<String, List<String>>().build(),
                null,
                null,
                null,
                jsonMapper);
        info = builder.create(new Message("routingkey", jsonMapper.writeValueAsBytes(msg)));
        assertNull(info.getId());
    }

    @Test
    public void shouldGetIdReturnsConcatenatedStr() throws JsonProcessingException {
        Map<String, Object> msg = new ImmutableMap.Builder<String, Object>()
                .put("f1", "v1")
                .put("f2", "v2")
                .put("f3", "v3")
                .build();

        DefaultIndexInfoBuilder builder = new DefaultIndexInfoBuilder(
                null,
                new ImmutableMap.Builder<String, List<String>>().put("routingkey", Lists.newArrayList("f1", "f2")).build(),
                null,
                null,
                null,
                jsonMapper);
        IndexInfo info = builder.create(new Message("routingkey", jsonMapper.writeValueAsBytes(msg)));
        assertEquals(info.getId(), "v1v2");
    }

    @Test
    public void shouldGetIdReturnsConcatedStrWithTimeslice() throws JsonProcessingException {
        DateTime dt = new DateTime("2014-10-12T12:12:12.000Z");

        Map<String, Object> msg = new ImmutableMap.Builder<String, Object>()
                .put("f1", "v1")
                .put("f2", "v2")
                .put("f3", "v3")
                .put("ts", dt.getMillis())
                .build();

        DefaultIndexInfoBuilder builder = new DefaultIndexInfoBuilder(
                null,
                new ImmutableMap.Builder<String, List<String>>().put("routingkey", Lists.newArrayList("f1", "f2", "ts_minute")).build(),
                new TimestampField("ts", null),
                null,
                null,
                jsonMapper);
        IndexInfo info = builder.create(new Message("routingkey", jsonMapper.writeValueAsBytes(msg)));
        assertEquals(info.getId(), ("v1v2" + dt.getMillis() / 60000));
    }

    @Test
    public void testCreation() throws IOException {
        String desc = "{\n" +
                "    \"type\": \"default\",\n" +
                "    \"indexTypeMap\":{\"routingkey1\":\"index1:type1\", \"routingkey2\":\"index2:type2\"},\n" +
                "    \"idFields\":{\"routingkey\": [\"f1\", \"f2\", \"ts_minute\"]},\n" +
                "    \"timestamp\": {\"field\":\"ts\"},\n" +
                "    \"indexSuffixFormatter\":{\"type\": \"date\", \"properties\":{\"dateFormat\":\"YYYYMMdd\"}}\n" +
                "}";
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
        DateTime dt = new DateTime("2014-10-12T12:12:12.000Z");

        Map<String, Object> msg = new ImmutableMap.Builder<String, Object>()
                .put("f1", "v1")
                .put("f2", "v2")
                .put("f3", "v3")
                .put("ts", dt.getMillis())
                .build();
        IndexInfoBuilder builder = jsonMapper.readValue(desc, new TypeReference<IndexInfoBuilder>(){});
        IndexInfo info = builder.create(new Message("routingkey", jsonMapper.writeValueAsBytes(msg)));
        assertEquals(info.getId(), ("v1v2" + dt.getMillis() / 60000));
    }
}
