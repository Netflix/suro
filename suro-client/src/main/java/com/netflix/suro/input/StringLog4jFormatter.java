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

package com.netflix.suro.input;

import com.google.inject.Inject;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.TagKey;
import org.apache.log4j.spi.LoggingEvent;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Iterator;
import java.util.Map;

public class StringLog4jFormatter implements Log4jFormatter {
    public static final char fieldDelim = '\035';
    public static final char fieldEqual = '\002';

    private final ClientConfig config;
    private final DateTimeFormatter fmt;
    private String routingKey;

    @Inject
    public StringLog4jFormatter(ClientConfig config) {
        this.config = config;
        fmt = DateTimeFormat.forPattern(config.getLog4jDateTimeFormat());
    }

    @Override
    public String format(LoggingEvent event) {
        StringBuilder sb = new StringBuilder();
        sb.append(fmt.print(new DateTime())).append(fieldDelim);
        sb.append(event.getLevel()).append(fieldDelim).append(event.getLoggerName());

        Object obj = event.getMessage();

        routingKey = null;

        // time UTC^]Level^]Map
        if (obj instanceof Map) {
            Map map = (Map) event.getMessage();
            Iterator it = map.keySet().iterator();
            String key = null;
            while (it.hasNext()) {
                key = (String) it.next();
                sb.append(fieldDelim).append(key).append(fieldEqual).append(map.get(key));
                if (key.equalsIgnoreCase(TagKey.ROUTING_KEY)) {
                    routingKey = (String) map.get(key);
                }
            }
        } else {
            // time UTC^]Level^]String
            sb.append(fieldDelim).append(obj.toString());
        }

        // Extract exceptions
        String[] s = event.getThrowableStrRep();
        if (s != null && s.length > 0) {
            sb.append(fieldDelim).append("Exception").append(fieldEqual).append(s[0]);
            for (int i = 1; i < s.length; i++) {
                sb.append('\n').append(s[i]);
            }
        }

        return sb.toString();
    }

    @Override
    public String getRoutingKey() {
        return routingKey;
    }
}
