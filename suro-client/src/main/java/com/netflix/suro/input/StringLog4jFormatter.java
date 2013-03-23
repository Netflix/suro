package com.netflix.suro.input;

import com.netflix.suro.ClientConfig;
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

        // time UTC^]Level^]Map
        if (obj instanceof Map) {
            Map map = (Map) event.getMessage();
            Iterator it = map.keySet().iterator();
            Object key = null;
            while (it.hasNext()) {
                key = it.next();
                sb.append(fieldDelim).append(key.toString()).append(fieldEqual).append(map.get(key));
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
}
