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

import com.google.inject.Injector;
import com.netflix.governator.guice.LifecycleInjector;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestRoutingMap {
    @Test
    public void test() {
        String mapDesc = "{\n" +
                "    \"request_trace\": {\n" +
                "        \"where\": [\n" +
                "            \"sink1\",\n" +
                "            \"sink2\",\n" +
                "            \"sink3\"\n" +
                "        ]\n" +
                "    },\n" +
                "    \"nf_errors_log\": {\n" +
                "        \"where\": [\n" +
                "            \"sink3\",\n" +
                "            \"sink4\"\n" +
                "        ]\n" +
                "    }\n" +
                "}";

        Injector injector = LifecycleInjector.builder().createInjector();
        RoutingMap routingMap = injector.getInstance(RoutingMap.class);
        routingMap.build(mapDesc);
        assertTrue(
                Arrays.equals(
                        routingMap.getRoutingInfo("request_trace").getWhere().toArray(),
                        new String[]{"sink1", "sink2", "sink3"}));

        assertTrue(
                Arrays.equals(
                        routingMap.getRoutingInfo("nf_errors_log").getWhere().toArray(),
                        new String[]{"sink3", "sink4"}));
        assertNull(routingMap.getRoutingInfo("streaming"));

        // test error
        // map description changed with json syntax error
        // nothing should be changed
        mapDesc = "{\n" +
                "    \"request_trace\": {\n" +
                "        \"where\": [\n" +
                "            \"sink1\",\n" +
                "            \"sink2\",\n" +
                "            \"sink3\"\n" +
                "        ]\n" +
                "    },\n" +
                "    \"nf_errors_log\": {\n" +
                "        \"where\": [\n" +
                "            \"sink3\",\n" +
                "            \"sink4\"\n" +
                "        ]\n" +
                "    },\n" +
                "}";
        routingMap.build(mapDesc);
        assertTrue(
                Arrays.equals(
                        routingMap.getRoutingInfo("request_trace").getWhere().toArray(),
                        new String[]{"sink1", "sink2", "sink3"}));

        assertTrue(
                Arrays.equals(
                        routingMap.getRoutingInfo("nf_errors_log").getWhere().toArray(),
                        new String[]{"sink3", "sink4"}));
        assertNull(routingMap.getRoutingInfo("streaming"));

        // description changed
        mapDesc = "{\n" +
                "    \"request_trace\": {\n" +
                "        \"where\": [\n" +
                "            \"sink1\",\n" +
                "            \"sink2\",\n" +
                "            \"sink3\"\n" +
                "        ]\n" +
                "    }\n" +
                "}";
        routingMap.build(mapDesc);
        assertTrue(
                Arrays.equals(
                        routingMap.getRoutingInfo("request_trace").getWhere().toArray(),
                        new String[]{"sink1", "sink2", "sink3"}));

        assertNull(routingMap.getRoutingInfo("nf_errors_log"));
        assertNull(routingMap.getRoutingInfo("streaming"));
    }

}
