package com.netflix.suro.sink.elasticsearch;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.netflix.suro.message.Message;
import com.netflix.suro.sink.DataConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class DefaultIndexInfoBuilder implements IndexInfoBuilder {
    private static final Logger log = LoggerFactory.getLogger(DefaultIndexInfoBuilder.class);

    private final static TypeReference<Map<String, Object>> type = new TypeReference<Map<String, Object>>() {};

    private final ObjectMapper jsonMapper;
    private final Map<String, String> indexMap;
    private final Map<String, String> typeMap;
    private final Map<String, List<String>> idFieldsMap;
    private final TimestampField timestampField;
    private final IndexSuffixFormatter indexSuffixFormatter;
    private final DataConverter dataConverter;

    @JsonCreator
    public DefaultIndexInfoBuilder(
            @JsonProperty("indexTypeMap") Map<String, String> indexTypeMap,
            @JsonProperty("idFields") Map<String, List<String>> idFieldsMap,
            @JsonProperty("timestamp") TimestampField timestampField,
            @JsonProperty("indexSuffixFormatter") IndexSuffixFormatter indexSuffixFormatter,
            @JacksonInject DataConverter dataConverter,
            @JacksonInject ObjectMapper jsonMapper
    ) {
        if (indexTypeMap != null) {
            indexMap = Maps.newHashMap();
            typeMap = Maps.newHashMap();

            for (Map.Entry<String, String> entry : indexTypeMap.entrySet()) {
                String[] index_type = entry.getValue().split(":");
                indexMap.put(entry.getKey(), index_type[0]);
                if (index_type.length > 1) {
                    typeMap.put(entry.getKey(), index_type[1]);
                }
            }
        } else {
            this.indexMap = Maps.newHashMap();
            this.typeMap = Maps.newHashMap();
        }

        this.idFieldsMap = idFieldsMap;
        this.indexSuffixFormatter =
                indexSuffixFormatter == null ? new IndexSuffixFormatter(null, null) : indexSuffixFormatter;
        this.jsonMapper = jsonMapper;
        this.timestampField = timestampField;
        this.dataConverter = dataConverter;
    }

    @Override
    public IndexInfo create(final Message msg) {
        try {
            final Map<String, Object> msgMap;
            if (dataConverter != null) {
                msgMap = dataConverter.convert((Map<String, Object>) jsonMapper.readValue(msg.getPayload(), type));
            } else {
                msgMap = jsonMapper.readValue(msg.getPayload(), type);
            }

            return new IndexInfo() {
                private long ts = 0; //timestamp caching

                @Override
                public String getIndex() {
                    String index = indexMap.get(msg.getRoutingKey());
                    if (index == null) {
                        index = msg.getRoutingKey();
                    }
                    return index + indexSuffixFormatter.format(this);
                }

                @Override
                    public String getType() {
                    String type = typeMap.get(msg.getRoutingKey());
                    return type == null ? "default" : type;
                }

                @Override
                public Object getSource() {
                    if (dataConverter != null) {
                        return msgMap;
                    } else {
                        return new String(msg.getPayload());
                    }
                }

                @Override
                public String getId() {
                    if (idFieldsMap == null || !idFieldsMap.containsKey(msg.getRoutingKey())) {
                        return null;
                    } else {
                        StringBuilder sb = new StringBuilder();
                        for (String id : idFieldsMap.get(msg.getRoutingKey())) {
                            if (id.startsWith("ts_")) {
                                sb.append(TimestampSlice.valueOf(id).get(getTimestamp()));
                            } else {
                                sb.append(msgMap.get(id));
                            }
                        }
                        return sb.toString();
                    }
                }

                @Override
                public long getTimestamp() {
                    if (ts == 0 && timestampField != null) {
                        ts = timestampField.get(msgMap);
                    }

                    return ts;
                }
            };
        } catch (Exception e) {
            log.error("Exception on parsing message", e);
            return null;
        }
    }

    @Override
    public String getActionMetadata(IndexInfo info) {
        if (!Strings.isNullOrEmpty(info.getId())) {
            return String.format(
                "{ \"create\" : { \"_index\" : \"%s\", \"_type\" : \"%s\", \"_id\" : \"%s\" } }",
                info.getIndex(), info.getType(), info.getId());
        } else {
            return String.format(
                "{ \"create\" : { \"_index\" : \"%s\", \"_type\" : \"%s\"} }",
                info.getIndex(), info.getType());
        }
    }

    @Override
    public String getSource(IndexInfo info) throws JsonProcessingException {
        if (info.getSource() instanceof Map) {
            return jsonMapper.writeValueAsString(info.getSource());
        } else {
            return info.getSource().toString();
        }
    }

    @Override
    public String getIndexUri(IndexInfo info) {
        return info.getId() != null ?
            String.format(
                "/%s/%s/%s",
                info.getIndex(),
                info.getType(),
                info.getId()) :
            String.format(
                "/%s/%s/",
                info.getIndex(),
                info.getType());
    }

    @Override
    public String getCommand() {
        return "create";
    }
}
