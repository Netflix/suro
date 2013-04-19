package com.netflix.suro.input;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.netflix.suro.ClientConfig;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestLog4jFormatter {
    @Test
    public void testString() {
        ClientConfig config = new ClientConfig();
        StringLog4jFormatter formatter = new StringLog4jFormatter(config);

        LoggingEvent event = mock(LoggingEvent.class);
        when(event.getLevel()).thenReturn(Level.INFO);
        when(event.getLoggerName()).thenReturn("TestLogger");
        when(event.getMessage()).thenReturn(new String("TestMessage"));
        when(event.getThrowableStrRep()).thenReturn(new String[]{"StackTrace0", "StackTrace1"});

        String[] messages = formatter.format(event).split("\035");
        // can't compare datetime because of millisecond
        // just check the time with second
        DateTime now = new DateTime();
        DateTimeFormatter fmt = DateTimeFormat.forPattern(config.getLog4jDateTimeFormat());
        String nowStr = fmt.print(now);
        assertEquals(nowStr.split(",")[0], messages[0].split(",")[0]);


        String[] answers = new String[]{"", "INFO", "TestLogger", "TestMessage", "Exception\002StackTrace0\nStackTrace1"};
        for (int i = 1; i < messages.length; ++i) {
            assertEquals(messages[i], answers[i]);
        }
    }

    @Test
    public void testJson() throws IOException {
        ClientConfig config = new ClientConfig();
        JsonLog4jFormatter formatter = new JsonLog4jFormatter(config);

        Map<String, Object> logEvent = Maps.newHashMap();
        logEvent.put("field1", "value1");
        logEvent.put("field2", 100);

        LoggingEvent event = mock(LoggingEvent.class);
        when(event.getLevel()).thenReturn(Level.INFO);
        when(event.getLoggerName()).thenReturn("TestLogger");
        when(event.getMessage()).thenReturn(logEvent);
        when(event.getThrowableStrRep()).thenReturn(new String[]{"StackTrace0", "StackTrace1"});

        Map<String, Object> formattedEvent = new ObjectMapper().readValue(
                formatter.format(event),
                new TypeReference<Map<String, Object>>(){});

        assertEquals(formattedEvent.get("field1"), "value1");
        assertEquals(formattedEvent.get("field2"), 100);
        assertEquals(formattedEvent.get("logLevel"), "INFO");
        assertEquals(formattedEvent.get("class"), "TestLogger");
        assertEquals(formattedEvent.get("Exception").toString(), "[StackTrace0, StackTrace1]");

        // can't compare datetime because of millisecond
        // just check the time with second
        DateTime now = new DateTime();
        DateTimeFormatter fmt = DateTimeFormat.forPattern(config.getLog4jDateTimeFormat());
        String nowStr = fmt.print(now);
        assertEquals(nowStr.split(",")[0], ((String)formattedEvent.get("datetime")).split(",")[0]);
    }
}
