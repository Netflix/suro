package com.netflix.suro.routing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.config.DynamicStringProperty;
import com.netflix.governator.annotations.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.Map;

/**
 * {@link com.netflix.config.DynamicProperty} driven routing map configuration.  Whenever a change is made to the
 * dynamic property of routing rules, this module will parse the JSON and set a new configuration on the
 * main {@link RoutingMap}.
 *
 * @author elandau
 */
@Singleton
public class DynamicPropertyRoutingMapConfigurator {
    public static final String ROUTING_MAP_PROPERTY = "SuroServer.routingMap";

    private static Logger LOG = LoggerFactory.getLogger(DynamicPropertyRoutingMapConfigurator.class);

    private final RoutingMap    routingMap;
    private final ObjectMapper  jsonMapper;

    @Configuration(ROUTING_MAP_PROPERTY)
    private String initialRoutingMap;

    @Inject
    public DynamicPropertyRoutingMapConfigurator(
        RoutingMap routingMap,
        ObjectMapper jsonMapper) {
        this.routingMap = routingMap;
        this.jsonMapper = jsonMapper;
    }

    @PostConstruct
    public void init() {
        DynamicStringProperty routingMapFP = new DynamicStringProperty(ROUTING_MAP_PROPERTY, initialRoutingMap) {
            @Override
            protected void propertyChanged() {
                buildMap(get(), false);
            }
        };

        buildMap(routingMapFP.get(), true);
    }

    private void buildMap(String map, boolean initStart) {
        try {
            LOG.debug("building routing map: {}", map);
            Map<String, RoutingMap.RoutingInfo> routes = jsonMapper.readValue(
                    map,
                    new TypeReference<Map<String, RoutingMap.RoutingInfo>>() {});
            routingMap.set(routes);
            if(initStart) {
                LOG.info("applied initial routing map: {}", map);
            } else {
                LOG.warn("applied updated routing map: {}", map);
            }
        } catch (Throwable e) {
            LOG.error("failed to build routing map", e);
        }
    }
}
