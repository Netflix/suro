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

import com.netflix.suro.message.MessageContainer;

import java.util.Map;

/**
 * An implementation of {@link MessageConverter} that converts the payload of a {@link com.netflix.suro.message.MessageContainer}
 * to a {@link java.util.Map} object with the assumption that the payload is a serialized JSON object.
 */
public class JsonMapConverter implements MessageConverter<Map>{
    public final static String TYPE = "jsonmap";

    @Override
    public Map convert(MessageContainer message) {
        try {
            return message.getEntity(Map.class);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format("Can't convert the given message with routing key %s to a JSON map: %s",
                    message.getRoutingKey(),
                    e.getMessage()),
                e);
        }
    }
}
