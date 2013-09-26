package com.netflix.suro.sink;

import java.util.Map;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.netflix.config.DynamicStringProperty;
import com.netflix.governator.annotations.Configuration;
import com.netflix.suro.routing.FastPropertyRoutingMapConfigurator;

/**
 * FastProperty driven sink configuration.  Whenever a change is made to the
 * fast property this module will parse the JSON and set a new configuration on the
 * main SinkManager.
 * 
 * @author elandau
 */
public class FastPropertySinkConfigurator {
    private static Logger log = LoggerFactory.getLogger(FastPropertyRoutingMapConfigurator.class);
    
    private static final String SINK_PROPERTY = "SuroServer.sinkConfig";
    
    private final SinkManager     sinkManager;
    private final ObjectMapper    jsonMapper;

    @Configuration(SINK_PROPERTY)
    private String initialSink;
    
    @Inject
    public FastPropertySinkConfigurator(
            SinkManager  sinkManager,
            ObjectMapper jsonMapper) {
        this.sinkManager = sinkManager;
        this.jsonMapper  = jsonMapper;
    }
    
    @PostConstruct
    public void init() {
        new DynamicStringProperty(SINK_PROPERTY, initialSink) {
            @Override
            protected void propertyChanged() {
                try {
                    Map<String, Sink> newSinkMap = jsonMapper.readValue(get(), new TypeReference<Map<String, Sink>>(){});
                    if (newSinkMap.containsKey("default") == false) {
                        throw new IllegalStateException("default sink should be defined");
                    }
                    
                    sinkManager.set(newSinkMap);
                } 
                catch (Exception e) {
                    log.error("Exception on building SinkManager: " + e.getMessage(), e);
                }
            }
        };
    }
}
