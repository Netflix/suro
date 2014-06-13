package com.netflix.suro.sink.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nullable;
import java.util.Properties;

public class IndexSuffixFormatter {
    private final Function<IndexInfo, String> formatter;

    @JsonCreator
    public IndexSuffixFormatter(
            @JsonProperty("type") String type,
            @JsonProperty("properties") Properties props) {
        if (type == null) {
            formatter = new Function<IndexInfo, String>() {
                @Nullable
                @Override
                public String apply(@Nullable IndexInfo input) {
                    return "";
                }
            };
        } else if (type.equals("date")) {
            final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(props.getProperty("dateFormat"));

            formatter = new Function<IndexInfo, String>() {

                @Nullable
                @Override
                public String apply(@Nullable IndexInfo input) {
                    return dateTimeFormatter.print(input.getTimestamp());
                }
            };
        } else {
            throw new RuntimeException("unsupported type: " + type);
        }
    }

    public String format(IndexInfo info) {
        return formatter.apply(info);
    }
}
