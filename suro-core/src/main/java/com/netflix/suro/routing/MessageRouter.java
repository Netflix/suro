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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.TagKey;
import com.netflix.suro.input.SuroInput;
import com.netflix.suro.message.DefaultMessageContainer;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.routing.RoutingMap.Route;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.SinkManager;

import java.util.List;

/**
 * Message routing module according to {@link RoutingMap}
 *
 * @author jbae
 */
@Singleton
public class MessageRouter {
    private final RoutingMap routingMap;
    private final SinkManager sinkManager;
    private final ObjectMapper jsonMapper;

    @Inject
    public MessageRouter(
            RoutingMap routingMap,
            SinkManager sinkManager,
            ObjectMapper jsonMapper) {
        this.routingMap = routingMap;
        this.sinkManager = sinkManager;
        this.jsonMapper = jsonMapper;

        Monitors.registerObject(this);
    }

    public void process(SuroInput input, MessageContainer msg) throws Exception {
        if (Strings.isNullOrEmpty(msg.getRoutingKey())) {
            DynamicCounter.increment(
                    MonitorConfig.builder(TagKey.DROPPED_COUNT).withTag("reason", "emptyRoutingKey").build());
            return; // discard message
        }

        DynamicCounter.increment(
            MonitorConfig
                .builder(TagKey.RECV_COUNT)
                .withTag("routingKey", msg.getRoutingKey())
                .build());

        RoutingMap.RoutingInfo info = routingMap.getRoutingInfo(msg.getRoutingKey());

        if (info == null) {
            Sink defaultSink = sinkManager.getSink("default");
            input.setPause(defaultSink.checkPause());
            defaultSink.writeTo(msg);
        } else if (info.doFilter(msg)) {
            List<Route> routes = info.getWhere();
            for (Route route : routes) {
                if (route.doFilter(msg)) {
                    Sink sink = sinkManager.getSink(route.getSink());
                    input.setPause(sink.checkPause());
                    if (!Strings.isNullOrEmpty(route.getAlias())) {
                        sink.writeTo(
                                new DefaultMessageContainer(
                                        new Message(route.getAlias(), msg.getMessage().getPayload()), jsonMapper));
                    } else {
                        sink.writeTo(msg);
                    }

                    DynamicCounter.increment(
                        MonitorConfig
                            .builder(TagKey.ATTEMPTED_COUNT)
                            .withTag("routingKey", msg.getRoutingKey())
                            .withTag("sinkId", route.getSink())
                            .build());
                }
            }
        }
    }
}
