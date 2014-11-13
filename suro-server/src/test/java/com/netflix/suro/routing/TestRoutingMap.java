/*
 * Copyright 2013 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.suro.routing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.*;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.suro.SuroPlugin;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.routing.RoutingMap.Route;
import com.netflix.suro.routing.RoutingMap.RoutingInfo;
import com.netflix.suro.sink.TestSinkManager.TestSink;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestRoutingMap {

    private static Injector injector = Guice.createInjector(
        new SuroPlugin() {
            @Override
            protected void configure() {
                this.addSinkType("TestSink", TestSink.class);
            }
        },
        new AbstractModule() {
            @Override
            protected void configure() {
                bind(ObjectMapper.class).to(DefaultObjectMapper.class);
            }
        },
        new RoutingPlugin()
    );

    private Map<String, RoutingInfo> getRoutingMap(String desc) throws Exception {
        return injector.getInstance(ObjectMapper.class).<Map<String, RoutingInfo>>readValue(
            desc,
            new TypeReference<Map<String, RoutingInfo>>() {});
    }

    @Test
    public void generateRoutingInfo() throws Exception {
        RoutingInfo route1 = new RoutingInfo(
            ImmutableList.<Route>of(
                new Route(
                    "sink2",
                    new XPathFilter("xpath(\"//customerInfo/country\") =~ \"(?i)^US\"", new JsonMapConverter()),
                    null
                ),

                new Route(
                    "sink3",
                    new XPathFilter("xpath(\"//responseInfo/status\") >= 400", new JsonMapConverter()),
                    null
                )
            ),
            null
        );

        System.out.println(new DefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(route1));
    }
    @Test
    public void testRoutingMapWithXPathFilter () throws Exception {
        String mapDesc = "{\n" +
            "  \"request_trace\" : {\n" +
            "    \"where\" : [ {\n" +
            "      \"sink\" : \"sink1\",\n" +
            "      \"filter\" : {\n" +
            "        \"type\" : \"xpath\",\n" +
            "        \"filter\" : \"xpath(\\\"//foo/bar\\\") =~ \\\"(?i)test\\\"\",\n" +
            "        \"converter\" : {\n" +
            "          \"type\" : \"jsonmap\"\n" +
            "        }\n" +
            "      }\n" +
            "    } ],\n" +
            "    \"filter\" : {\n" +
            "      \"type\" : \"regex\",\n" +
            "      \"regex\" : \"[a-b]+\"\n" +
            "    }\n" +
            "  }\n" +
            "}";

        Map<String, RoutingInfo> map = getRoutingMap(mapDesc);

        String routingKey = "request_trace";
        assertEquals("There should be one and only one key", 1, map.size());
        assertTrue("The only key is " + routingKey, map.containsKey(routingKey));

        RoutingInfo info = map.get(routingKey);
        List<Route> routes = info.getWhere();
        assertEquals("There should be only one sink", 1, routes.size());

        Route route = routes.get(0);
        assertEquals("sink1", route.getSink());

        Map<String, Map<String, String>> obj = Maps.newHashMap();
        obj.put("foo", ImmutableMap.of("bar", "tESt"));

        MessageContainer container = Mockito.mock(MessageContainer.class);
        Mockito.when(container.getEntity(Map.class)).thenReturn(obj);
        assertEquals(true, route.getFilter().doFilter(container));
    }

    @Test
    public void test() throws Exception {
        String mapDesc = "{\n" +
            "    \"request_trace\": {\n" +
            "        \"where\": [\n" +
            "            {\"sink\":\"sink1\"},\n" +
            "            {\"sink\":\"sink2\"},\n" +
            "            {\"sink\":\"sink3\"}\n" +
            "        ]\n" +
            "    },\n" +
            "    \"nf_errors_log\": {\n" +
            "        \"where\": [\n" +
            "            {\"sink\":\"sink3\"},\n" +
            "            {\"sink\":\"sink4\"}\n" +
            "        ]\n" +
            "    }\n" +
            "}";

        RoutingMap routingMap = new RoutingMap();
        routingMap.set(getRoutingMap(mapDesc));
        assertTrue(
            Arrays.equals(
                getSinkNames(routingMap.getRoutingInfo("request_trace").getWhere()),
                new String[]{"sink1", "sink2", "sink3"}));

        assertTrue(
            Arrays.equals(
                getSinkNames(routingMap.getRoutingInfo("nf_errors_log").getWhere()),
                new String[]{"sink3", "sink4"}));
        assertNull(routingMap.getRoutingInfo("streaming"));

        // test error
        // map description changed with json syntax error
        // nothing should be changed
        mapDesc = "{\n" +
            "    \"request_trace\": {\n" +
            "        \"where\": [\n" +
            "            {\"sink\":\"sink1\"},\n" +
            "            {\"sink\":\"sink2\"},\n" +
            "            {\"sink\":\"sink3\"}\n" +
            "        ]\n" +
            "    },\n" +
            "    \"nf_errors_log\": {\n" +
            "        \"where\": [\n" +
            "            {\"sink\":\"sink3\"},\n" +
            "            {\"sink\":\"sink4\"}\n" +
            "        ]\n" +
            "    }\n" +
            "}";
        routingMap.set(getRoutingMap(mapDesc));
        assertTrue(
            Arrays.equals(
                getSinkNames(routingMap.getRoutingInfo("request_trace").getWhere()),
                new String[]{"sink1", "sink2", "sink3"}));

        assertTrue(
            Arrays.equals(
                getSinkNames(routingMap.getRoutingInfo("nf_errors_log").getWhere()),
                new String[]{"sink3", "sink4"}));
        assertNull(routingMap.getRoutingInfo("streaming"));

        // description changed
        mapDesc = "{\n" +
            "    \"request_trace\": {\n" +
            "        \"where\": [\n" +
            "            {\"sink\":\"sink1\"},\n" +
            "            {\"sink\":\"sink2\"},\n" +
            "            {\"sink\":\"sink3\"}\n" +
            "        ]\n" +
            "    }\n" +
            "}";
        routingMap.set(getRoutingMap(mapDesc));
        assertTrue(
            Arrays.equals(
                getSinkNames(routingMap.getRoutingInfo("request_trace").getWhere()),
                new String[]{"sink1", "sink2", "sink3"}));

        assertNull(routingMap.getRoutingInfo("nf_errors_log"));
        assertNull(routingMap.getRoutingInfo("streaming"));
    }

    private Object[] getSinkNames(List<Route> routes) {
        return Lists.newArrayList(Collections2.transform(routes, new Function<Route, String>() {
            @Override
            @Nullable
            public String apply(@Nullable Route input) {
                return input.getSink();
            }
        })).toArray();
    }

    @Test
    public void testDefaultRoutingIsOptional() throws Exception {
        // description changed
        String mapDesc = "{\n" +
            "    \"request_trace\": {\n" +
            "        \"where\": [\n" +
            "            {\"sink\":\"sink1\"},\n" +
            "            {\"sink\":\"sink2\"},\n" +
            "            {\"sink\":\"sink3\"}\n" +
            "        ]\n" +
            "    }\n" +
            "}";

        RoutingMap routingMap = new RoutingMap();
        routingMap.set(getRoutingMap(mapDesc));
        assertTrue(
            Arrays.equals(
                getSinkNames(routingMap.getRoutingInfo("request_trace").getWhere()),
                new String[]{"sink1", "sink2", "sink3"}));

        // Verify that
        assertNull(routingMap.getRoutingInfo("nf_errors_log"));
        assertNull(routingMap.getRoutingInfo("streaming"));
    }

    @Test
    public void testDefaultRouting() throws Exception {
        String mapDesc = "{\n" +
            "    \"__default__\": {\n" +
            "       \"where\": [\n" +
            "         {\"sink\": \"sinkD1\"},\n" +
            "         {\"sink\": \"sinkD2\"}\n" +
            "       ]\n" +
            "    },\n" +
            "    \"request_trace\": {\n" +
            "        \"where\": [\n" +
            "            {\"sink\":\"sink1\"},\n" +
            "            {\"sink\":\"sink2\"},\n" +
            "            {\"sink\":\"sink3\"}\n" +
            "        ]\n" +
            "    },\n" +
            "    \"nf_errors_log\": {\n" +
            "        \"where\": [\n" +
            "            {\"sink\":\"sink3\"},\n" +
            "            {\"sink\":\"sink4\"}\n" +
            "        ]\n" +
            "    }\n" +
            "}";

        RoutingMap routingMap = new RoutingMap();
        routingMap.set(getRoutingMap(mapDesc));

        // Verify that default routes are in place
        assertArrayEquals(getSinkNames(routingMap.getRoutingInfo("some non existing key").getWhere()), new String[]{"sinkD1", "sinkD2"});

        // Ensure that overrides still work
        assertTrue(
            Arrays.equals(
                getSinkNames(routingMap.getRoutingInfo("request_trace").getWhere()),
                new String[]{"sink1", "sink2", "sink3"}));

        assertTrue(
            Arrays.equals(
                getSinkNames(routingMap.getRoutingInfo("nf_errors_log").getWhere()),
                new String[]{"sink3", "sink4"}));

    }
}
