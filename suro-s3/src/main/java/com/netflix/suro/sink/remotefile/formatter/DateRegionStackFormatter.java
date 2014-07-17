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

package com.netflix.suro.sink.remotefile.formatter;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.suro.sink.remotefile.RemotePrefixFormatter;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * It would be useful to append region and stack information to the file path
 * when we upload files to AWS S3. region and stack can be injected through
 * Jackson {@link com.fasterxml.jackson.databind.ObjectMapper}.
 *
 * @author jbae
 */
public class DateRegionStackFormatter implements RemotePrefixFormatter {
    public static final String TYPE = "DateRegionStack";

    private final DateTimeFormatter format;
    private String region;
    private String stack;

    @JsonCreator
    public DateRegionStackFormatter(
            @JsonProperty("date") String dateFormat,
            @JsonProperty("region") @JacksonInject("region") String region,
            @JsonProperty("stack")  @JacksonInject("stack") String stack) {
        this.format = DateTimeFormat.forPattern(dateFormat);
        this.region = region;
        this.stack = stack;
    }

    @Override
    public String get() {
        StringBuilder sb = new StringBuilder();
        sb.append(format.print(new DateTime())).append('/')
                .append(region).append('/')
                .append(stack).append('/');

        return sb.toString();
    }
}
