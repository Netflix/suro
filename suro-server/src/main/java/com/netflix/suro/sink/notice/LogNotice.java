package com.netflix.suro.sink.notice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogNotice implements Notice<String> {
    public static final String TYPE = "log";

    private static Logger log = LoggerFactory.getLogger(LogNotice.class);

    @Override
    public void init() {

    }

    @Override
    public boolean send(String message) {
        log.info(message);

        return true;
    }

    @Override
    public String recv() {
        return null;
    }

    @Override
    public String getStat() {
        return null;
    }
}
