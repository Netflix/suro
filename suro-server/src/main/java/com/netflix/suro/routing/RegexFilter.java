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
import com.netflix.suro.message.Message;
import com.netflix.suro.message.SerDe;
import com.netflix.suro.message.StringSerDe;

import java.util.regex.Pattern;

public class RegexFilter implements Filter {
    private final Pattern filterPattern;
    private final SerDe<String> serde = new StringSerDe();

    @JsonCreator
    public RegexFilter(@JsonProperty("regex") String regex) {
        filterPattern = Pattern.compile(regex);
    }

    @Override
    public boolean doFilter(Object obj) {
        if (obj instanceof Message) {
            String strMsg = serde.deserialize(((Message) obj).getPayload());
            return filterPattern.matcher(strMsg).find();
        } else {
            return filterPattern.matcher(obj.toString()).find();
        }
    }
}
