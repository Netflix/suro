/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.suro.routing;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.suro.message.MessageContainer;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class FilterTestUtil {
    private static final Splitter PATH_SPLITTER = Splitter.on("/").omitEmptyStrings().trimResults();

    // Returns a message container that has the given routing key , and payload that has the given path and given value
    public static MessageContainer makeMessageContainer(String routingKey, String path, Object value) throws Exception {
        MessageContainer container = mock(MessageContainer.class);
        when(container.getRoutingKey()).thenReturn(routingKey);

        if(path == null) {
            return container;
        }
        List<String> steps = Lists.newArrayList(PATH_SPLITTER.split(path));
        Map<String, Object> map = Maps.newHashMap();
        Map<String, Object> current = map;

        for(int i = 0; i < steps.size() - 1; i++) {
            String step = steps.get(i);
            Map<String, Object> obj = Maps.newHashMap();
            current.put(step, obj);
            current = obj;
        }
        current.put(steps.get(steps.size() - 1), value);

        when(container.getEntity(Map.class)).thenReturn(map);

        return container;
    }
}
