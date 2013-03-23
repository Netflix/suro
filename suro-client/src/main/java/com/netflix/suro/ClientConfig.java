package com.netflix.suro;

import com.netflix.governator.annotations.Configuration;

public class ClientConfig {
    public static final String CONNECTION_TIMEOUT = "SuroClient.connectionTimeout";
    @Configuration(CONNECTION_TIMEOUT)
    private int connectionTimeout = 20000; // millisecond
    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public static final String CONNECTION_SWEEP_INTERVAL = "SuroClient.connectionSweepInterval";
    @Configuration(CONNECTION_SWEEP_INTERVAL)
    private int connectionSweepInterval = 3600; // second
    public int getConnectionSweepInterval() {
        return connectionSweepInterval;
    }

    public static final String LOG4J_FORMATTER = "SuroClient.log4jFormatter";
    @Configuration(LOG4J_FORMATTER)
    private String log4jFormatter = "com.netflix.suro.input.StringLog4jFormatter";
    public String getLog4jFormatter() {
        return log4jFormatter;
    }

    public static final String LOG4J_DATETIMEFORMAT = "SuroClient.log4jDateTimeFormat";
    @Configuration(LOG4J_DATETIMEFORMAT)
    private String log4jDateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss,SSS";
    public String getLog4jDateTimeFormat() {
        return log4jDateTimeFormat;
    }
}
