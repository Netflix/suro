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
import com.netflix.suro.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Tracks the main routing map as a volatile immutable Map of route key to filter and destination
 * 
 * @author metacret
 * @author elandau
 *
 */
@Singleton
public class RoutingMap {
    static Logger log = LoggerFactory.getLogger(RoutingMap.class);

    public static class RoutingInfo {
        private final List<String> where;
        private final Filter filter;

        @JsonCreator
        public RoutingInfo(
                @JsonProperty("where") List<String> where,
                @JsonProperty("filter") Filter filter
        ) {
            this.where  = where;
            this.filter = filter;
        }

        public List<String> getWhere() { return where; }
        public boolean doFilter(Message message) {
            return filter != null ? filter.doFilter(message) : true;
        }
    }
    
    private volatile Map<String, RoutingInfo> routingMap = Maps.newHashMap();

    public RoutingInfo getRoutingInfo(String routingKey) {
        return routingMap.get(routingKey);
    }

    public void set(Map<String, RoutingInfo> routes) {
        this.routingMap = ImmutableMap.copyOf(routes);
    }
}
