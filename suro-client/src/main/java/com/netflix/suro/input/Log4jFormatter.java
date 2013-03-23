package com.netflix.suro.input;

import org.apache.log4j.spi.LoggingEvent;

public interface Log4jFormatter {
    String format(LoggingEvent event);
}
