package com.netflix.suro.routing;

import java.util.Map;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.config.DynamicStringProperty;
import com.netflix.governator.annotations.Configuration;
import com.netflix.suro.routing.RoutingMap.RoutingInfo;

/**
 * FastProperty driven routing map configuration.  Whenever a change is made to the
 * fast property this module will parse the JSON and set a new configuration on the
 * main RoutingMap.
 * 
 * @author elandau
 */
@Singleton
public class FastPropertyRoutingMapConfigurator {
    private static final String ROUTING_MAP_PROPERTY = "SuroServer.routingMap";
    
    private static Logger LOG = LoggerFactory.getLogger(FastPropertyRoutingMapConfigurator.class);
    
    private final RoutingMap    routingMap;
    private final ObjectMapper  jsonMapper;

    @Configuration(ROUTING_MAP_PROPERTY)
    private String initialRoutingMap;
    
    @Inject
    public FastPropertyRoutingMapConfigurator(
            RoutingMap routingMap,
            ObjectMapper jsonMapper) {
        this.routingMap = routingMap;
        this.jsonMapper = jsonMapper;
    }
    
    @PostConstruct
    public void init() {
        new DynamicStringProperty(ROUTING_MAP_PROPERTY, initialRoutingMap) {
            @Override
            protected void propertyChanged() {
                try {
                    Map<String, RoutingInfo> routes = jsonMapper.<Map<String, RoutingInfo>>readValue(
                            get(),
                            new TypeReference<Map<String, RoutingInfo>>() {});
                    routingMap.set(routes);
                } catch (Exception e) {
                    LOG.info("Error reading routing map from fast property", e);
                }
            }
        };
    }
}
