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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.TagKey;
import com.netflix.suro.jackson.DefaultObjectMapper;
import org.apache.log4j.spi.LoggingEvent;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class JsonLog4jFormatter implements Log4jFormatter {
    private final ClientConfig config;
    private final DateTimeFormatter fmt;
    private final ObjectMapper jsonMapper;
    private final StringLog4jFormatter stringFormatter;

    private String routingKey;

    @Monitor(name = "jsonParsingError", type = DataSourceType.COUNTER)
    private AtomicLong jsonParsingError = new AtomicLong(0);

    public JsonLog4jFormatter(ClientConfig config) {
        this(config, null);
    }
    
    @Inject
    public JsonLog4jFormatter(ClientConfig config, ObjectMapper jsonMapper) {
        this.config     = config;
        if (jsonMapper == null)
            this.jsonMapper = new DefaultObjectMapper();
        else
            this.jsonMapper = jsonMapper;
        fmt = DateTimeFormat.forPattern(config.getLog4jDateTimeFormat());
        stringFormatter = new StringLog4jFormatter(config);

        Monitors.registerObject(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public String format(LoggingEvent event) {
        Object obj = event.getMessage();

        routingKey = null;

        if (obj instanceof Map) {
            Map map = (Map) event.getMessage();
            DateTime now = new DateTime();
            map.put("ts", now.getMillis());
            map.put("datetime", fmt.print(now));
            map.put("logLevel", event.getLevel().toString());
            map.put("class", event.getLoggerName());

            routingKey = (String) map.get(TagKey.ROUTING_KEY);

            // Extract exceptions
            String[] s = event.getThrowableStrRep();
            if (s != null && s.length > 0) {
                map.put("Exception", s);
            }
            try {
                return jsonMapper.writeValueAsString(map);
            } catch (JsonProcessingException e) {
                jsonParsingError.incrementAndGet();
                return stringFormatter.format(event);
            }
        } else {
            jsonParsingError.incrementAndGet();
            return stringFormatter.format(event);
        }
    }

    @Override
    public String getRoutingKey() {
        return routingKey;
    }
}
