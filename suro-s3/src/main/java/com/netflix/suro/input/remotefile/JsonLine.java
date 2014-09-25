package com.netflix.suro.input.remotefile;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.netflix.suro.input.RecordParser;
import com.netflix.suro.message.DefaultMessageContainer;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JsonLine implements RecordParser {
    private static Logger log = LoggerFactory.getLogger(JsonLine.class);

    public static final String TYPE = "jsonline";

    private final String routingKeyField;
    private final String routingKey;
    private final ObjectMapper jsonMapper;

    @JsonCreator
    public JsonLine(
            @JsonProperty("routingKey") String routingKey,
            @JsonProperty("routingKeyField") String routingKeyField,
            @JacksonInject ObjectMapper jsonMapper
    ) {
        this.routingKey = routingKey;
        this.routingKeyField = routingKeyField;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public List<MessageContainer> parse(String data) {
        if (routingKey != null) {
            return new ImmutableList.Builder<MessageContainer>()
                    .add(new DefaultMessageContainer(
                            new Message(routingKey, data.getBytes()),
                            jsonMapper))
                    .build();
        } else {
            try {
                Map<String, Object> record = jsonMapper.readValue(data, S3Consumer.typeReference);
                String routingKeyOnRecord = record.get(routingKeyField).toString();
                if (Strings.isNullOrEmpty(routingKeyOnRecord)) {
                    routingKeyOnRecord = routingKey;
                }
                if (!Strings.isNullOrEmpty(routingKeyOnRecord)) {
                    return new ImmutableList.Builder<MessageContainer>()
                            .add(new DefaultMessageContainer(
                                    new Message(routingKeyOnRecord, data.getBytes()),
                                    jsonMapper))
                            .build();
                } else {
                    return new ArrayList<MessageContainer>();
                }
            } catch (IOException e) {
                log.error("Exception on parsing: " + e.getMessage(), e);
                return new ArrayList<MessageContainer>();
            }
        }
    }
}
