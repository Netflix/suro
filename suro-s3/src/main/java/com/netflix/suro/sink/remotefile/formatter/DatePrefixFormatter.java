package com.netflix.suro.sink.remotefile.formatter;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DatePrefixFormatter implements PrefixFormatter {
    private final DateTimeFormatter formatter;

    public DatePrefixFormatter(String formatString) {
        this.formatter = DateTimeFormat.forPattern(formatString);
    }

    @Override
    public String format() {
        return formatter.print(new DateTime());
    }
}
