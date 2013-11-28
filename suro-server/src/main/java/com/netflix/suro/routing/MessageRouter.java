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

import com.google.inject.Inject;
import com.netflix.governator.guice.lazy.LazySingleton;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.sink.SinkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Message routing module according to {@link RoutingMap}
 *
 * @author jbae
 */
@LazySingleton
public class MessageRouter {
    static Logger log = LoggerFactory.getLogger(MessageRouter.class);

    private final RoutingMap routingMap;
    private final SinkManager sinkManager;

    @Inject
    public MessageRouter(
            RoutingMap routingMap,
            SinkManager sinkManager) {
        this.routingMap = routingMap;
        this.sinkManager = sinkManager;

        Monitors.registerObject(this);
    }

    public void process(MessageContainer msg) {
        RoutingMap.RoutingInfo info = routingMap.getRoutingInfo(msg.getRoutingKey());

        if (info == null) {
            sinkManager.getSink("default").writeTo(msg);
        } else if (info != null && info.doFilter(msg)) {
            List<String> sinkList = info.getWhere();
            for (String sink : sinkList) {
                sinkManager.getSink(sink).writeTo(msg);
            }
        }
    }
}
