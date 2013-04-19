package com.netflix.suro.routing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.serde.SerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class RoutingMap {
    static Logger log = LoggerFactory.getLogger(RoutingMap.class);

    public static class RoutingInfo {
        private final List<String> where;
        private final Converter converter;
        private final Filter filter;

        @JsonCreator
        public RoutingInfo(
                @JsonProperty("where") List<String> where,
                @JsonProperty("converter") Converter converter,
                @JsonProperty("filter") Filter filter
        ) {
            this.where = where;
            this.converter = converter;
            this.filter = filter;
        }

        public List<String> getWhere() { return where; }
        public Converter getConverter() { return converter; }
        public boolean doFilter(Message message, SerDe serde) {
            return filter != null ? filter.doFilter(message, serde) : true;
        }
    }

    private AtomicReference<Map<String, RoutingInfo>> routingMap =
            new AtomicReference<Map<String, RoutingInfo>>(Maps.<String, RoutingInfo>newHashMap());

    private final ObjectMapper jsonMapper;

    @Inject
    public RoutingMap(ObjectMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public void build(String mapDesc) {
        try {
            Map<String, RoutingInfo> newRoutingMap = jsonMapper.<Map<String, RoutingInfo>>readValue(
                    mapDesc,
                    new TypeReference<Map<String, RoutingInfo>>() {
                    });

            this.routingMap.set(newRoutingMap);
        } catch (IOException e) {
            log.error("IOException on building RoutingMap: " + e.getMessage(), e);
        }
    }

    public RoutingInfo getRoutingInfo(String routingKey) {
        return routingMap.get().get(routingKey);
    }
}
