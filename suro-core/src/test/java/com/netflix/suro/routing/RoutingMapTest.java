package com.netflix.suro.routing;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RoutingMapTest {

    @Test
    public void testSingleKey() {
        Map<String, RoutingMap.RoutingInfo> routes = new HashMap<>();
        RoutingMap.Route route = new RoutingMap.Route("kafka", null, null);
        RoutingMap.RoutingInfo info = new RoutingMap.RoutingInfo(Arrays.asList(route), null);
        routes.put("key1", info);

        RoutingMap routingMap = new RoutingMap();
        routingMap.set(routes);
        Assert.assertEquals(1, routingMap.getRoutingMap().size());
        Assert.assertTrue(routingMap.getRoutingMap().containsKey("key1"));
    }

    @Test
    public void testCommaSeparatedKey() {
        Map<String, RoutingMap.RoutingInfo> routes = new HashMap<>();
        RoutingMap.Route route = new RoutingMap.Route("kafka", null, null);
        RoutingMap.RoutingInfo info = new RoutingMap.RoutingInfo(Arrays.asList(route), null);
        routes.put("key1,key2,    key3  ", info);

        RoutingMap routingMap = new RoutingMap();
        routingMap.set(routes);
        Assert.assertEquals(3, routingMap.getRoutingMap().size());
        Assert.assertTrue(routingMap.getRoutingMap().containsKey("key1"));
        Assert.assertTrue(routingMap.getRoutingMap().containsKey("key2"));
        Assert.assertTrue(routingMap.getRoutingMap().containsKey("key3"));
    }
}
