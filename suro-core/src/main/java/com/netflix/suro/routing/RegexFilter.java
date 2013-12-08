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
import com.netflix.suro.message.MessageContainer;

import java.util.regex.Pattern;

/**
 * An implementation of {@link Filter} that coerced message payload to a string, and applies
 * regex to the coerced string. The match is partial.
 *
 */
public class RegexFilter implements Filter {
    public static final String TYPE = "regex";
    public static final String JSON_PROPERTY_REGEX = "regex";

    private final Pattern filterPattern;

    @JsonCreator
    public RegexFilter(@JsonProperty(JSON_PROPERTY_REGEX) String regex) {
        filterPattern = Pattern.compile(regex);
    }

    @JsonProperty(JSON_PROPERTY_REGEX)
    public String getRegex() {
        return filterPattern.pattern();
    }

    /**
     *
     * @param message The message that is to be matched against the filter's regex pattern
     * @return true if the message's string representation contains the regex pattern. False otherwise.
     * @throws Exception If filtering fails. For example, if the given message can't be coerced into a string.
     */
    @Override
    public boolean doFilter(MessageContainer message) throws Exception {
        return filterPattern.matcher(message.getEntity(String.class)).find();
    }
}
