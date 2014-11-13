package com.netflix.suro.input.remotefile;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.suro.input.RecordParser;
import com.netflix.suro.message.DefaultMessageContainer;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CloudTrail implements RecordParser {
    private static Logger log = LoggerFactory.getLogger(CloudTrail.class);

    public static final String TYPE = "cloudtrail";

    private final ObjectMapper jsonMapper;
    private final String routingKey;

    @JsonCreator
    public CloudTrail(
            @JsonProperty("routingKey") String routingKey,
            @JacksonInject ObjectMapper jsonMapper
    ) {
        this.routingKey = routingKey == null ? "cloudtrail" : routingKey;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public List<MessageContainer> parse(String data) {
        List<MessageContainer> messages = new ArrayList<MessageContainer>();

        try {
            Map<String, Object> blob = jsonMapper.readValue(data, S3Consumer.typeReference);
            List<Map<String, Object>> records = (List<Map<String, Object>>) blob.get("Records");
            for (Map<String, Object> record : records) {
                messages.add(new DefaultMessageContainer(
                        new Message(routingKey, jsonMapper.writeValueAsBytes(record)),
                        jsonMapper));
            }
        } catch (Exception e) {
            log.error("Exception on parsing: " + e.getMessage(), e);
            DynamicCounter.increment(
                    MonitorConfig.builder("recordParseError").withTag("parserType", TYPE).build());
        }

        return messages;
    }
}
