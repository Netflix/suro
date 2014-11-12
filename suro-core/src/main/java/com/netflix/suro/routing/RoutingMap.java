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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Singleton;
import com.netflix.suro.message.MessageContainer;

import java.util.List;
import java.util.Map;

/**
 * A collection of {@link RoutingInfo}, each of which is mapped to a routing key.
 *
 * @author jbae
 * @author elandau
 *
 */
@Singleton
public class RoutingMap {
    public static final String KEY_FOR_DEFAULT_ROUTING = "__default__";

    public static class Route {
        public static final String JSON_PROPERTY_SINK = "sink";
        public static final String JSON_PROPERTY_FILTER = "filter";
        public static final String JSON_PROPERTY_ALIAS = "alias";

        private final String sink;
        private final Filter filter;
        private final String alias;

        @JsonCreator
        public Route(
            @JsonProperty(JSON_PROPERTY_SINK) String sink,
            @JsonProperty(JSON_PROPERTY_FILTER) Filter filter,
            @JsonProperty(JSON_PROPERTY_ALIAS) String alias) {
            this.sink   = sink;
            this.filter = filter;
            this.alias = alias;
        }

        @JsonProperty(JSON_PROPERTY_SINK)
        public String getSink() {
            return sink;
        }

        @JsonProperty(JSON_PROPERTY_FILTER)
        public Filter getFilter() {
            return filter;
        }

        @JsonProperty(JSON_PROPERTY_ALIAS)
        public String getAlias() { return alias; }

        public boolean doFilter(MessageContainer message) throws Exception {
            return filter == null || filter.doFilter(message);
        }

    }

    public static class RoutingInfo {
        public static final String JSON_PROPERTY_WHERE = "where";
        public static final String JSON_PROPERTY_FILTER = "filter";
        private final List<Route> where;
        private final Filter filter;

        @JsonCreator
        public RoutingInfo(
            @JsonProperty(JSON_PROPERTY_WHERE) List<Route> where,
            @JsonProperty(JSON_PROPERTY_FILTER) Filter filter
        ) {
            this.where  = where;
            this.filter = filter;
        }

        @JsonProperty(JSON_PROPERTY_WHERE)
        public List<Route> getWhere() {
            return where;
        }

        @JsonProperty(JSON_PROPERTY_FILTER)
        public Filter getFilter() {
            return filter;
        }

        /**
         * Filters given messages based on filters defined in
         * this object.
         * @param message the message to be filtered.
         * @return true if there is no filter found or if the filter returns true when applied to the message.
         * @throws Exception thrown if filtering ran into an unexpected failure
         */
        public boolean doFilter(MessageContainer message) throws Exception {
            return filter == null || filter.doFilter(message);
        }
    }

    private volatile ImmutableMap<String, RoutingInfo> routingMap = ImmutableMap.of();
    private volatile RoutingInfo defaultRoutingInfo = null;

    public RoutingInfo getRoutingInfo(String routingKey) {
        RoutingInfo info = routingMap.get(routingKey);
        if(info == null) {
            info = defaultRoutingInfo;
        }

        return info;
    }

    public void set(Map<String, RoutingInfo> routes) {
        // We assume only a single thread will call this method, so
        // there's no need to synchronize this method
        routingMap = ImmutableMap.copyOf(routes);
        defaultRoutingInfo = routingMap.get(KEY_FOR_DEFAULT_ROUTING);
    }

    public Map<String, RoutingInfo> getRoutingMap() {
        return routingMap;
    }
}