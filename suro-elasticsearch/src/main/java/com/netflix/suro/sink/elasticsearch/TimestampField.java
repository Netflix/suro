package com.netflix.suro.sink.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;

import java.util.Map;

public class TimestampField {
    private final String fieldName;
    private final FormatDateTimeFormatter formatter;

    @JsonCreator
    public TimestampField(@JsonProperty("field") String fieldName, @JsonProperty("format") String format) {
        if (format == null) {
            format = "dateOptionalTime";
        }

        this.fieldName = fieldName;
        this.formatter = Joda.forPattern(format);
    }

    public long get(Map<String, Object> msg) {
        Object value = msg.get(fieldName);

        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return formatter.parser().parseMillis(value.toString());
    }
}
