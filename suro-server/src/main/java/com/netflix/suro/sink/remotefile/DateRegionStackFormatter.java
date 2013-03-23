package com.netflix.suro.sink.remotefile;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateRegionStackFormatter implements RemotePrefixFormatter {
    public static final String TYPE = "DateRegionStack";

    private final DateTimeFormatter format;
    private final String region;
    private final String stack;

    @JsonCreator
    public DateRegionStackFormatter(
            @JsonProperty("date") String dateFormat,
            @JsonProperty("region") @JacksonInject("region") String region,
            @JsonProperty("stack") @JacksonInject("stack") String stack) {
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
