package com.netflix.suro.input;

import com.google.inject.Inject;
import com.netflix.suro.ClientConfig;
import org.apache.log4j.spi.LoggingEvent;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class StaticLog4jFormatter implements Log4jFormatter {
    private final DateTimeFormatter fmt;
    private final ClientConfig config;

    @Inject
    public StaticLog4jFormatter(ClientConfig config) {
        this.config = config;

        fmt = DateTimeFormat.forPattern(config.getLog4jDateTimeFormat());
    }

    @Override
    public String format(LoggingEvent event) {
        StringBuilder sb = new StringBuilder();
        sb.append(fmt.print(new DateTime())).append('\t');
        sb.append(event.getLevel()).append('\t').append(event.getLoggerName());

        Object obj = event.getMessage();

        sb.append('\t').append(obj.toString());

        // Extract exceptions
        String[] s = event.getThrowableStrRep();
        if (s != null && s.length > 0) {
            sb.append('\n').append(s[0]);
            for (int i = 1; i < s.length; i++) {
                sb.append('\n').append(s[i]);
            }
        }

        return sb.toString();
    }

    @Override
    public String getRoutingKey() {
        return config.getLog4jRoutingKey();
    }
}
