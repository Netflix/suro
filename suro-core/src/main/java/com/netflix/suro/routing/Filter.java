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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.queue.FileQueue4Sink;
import com.netflix.suro.queue.MemoryQueue4Sink;

/**
 * Interface for filtering messages. Implementation of this filter must be serializable into
 * JSON.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = RegexFilter.TYPE, value = RegexFilter.class),
    @JsonSubTypes.Type(name = XPathFilter.TYPE, value = XPathFilter.class),
    @JsonSubTypes.Type(name = RoutingKeyFilter.TYPE, value = RoutingKeyFilter.class)
})
public interface Filter {
    /**
     * Performs message matching to help determine if a message should be filtered.
     * @param message The message container that will be matched against
     * @return true if the given message matches an implementation's filtering rule. False otherwise.
     * @throws Exception if unexpected error happens
     */
    public boolean doFilter(MessageContainer message) throws Exception;
}