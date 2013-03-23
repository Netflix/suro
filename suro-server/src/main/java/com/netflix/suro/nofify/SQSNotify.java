package com.netflix.suro.nofify;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class SQSNotify implements Notify {
    public static final String TYPE = "sqs";

    private final List<String> queues;

    @JsonCreator
    public SQSNotify(
            @JsonProperty("queues") List<String> queues) {
        this.queues = queues;
    }

    @Override
    public boolean send(String message) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String recv() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
