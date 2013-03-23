package com.netflix.suro.sink.remotefile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StaticPrefixFormatter implements RemotePrefixFormatter {
    public static final String TYPE = "static";

    private final String prefix;

    @JsonCreator
    public StaticPrefixFormatter(@JsonProperty("prefix") String prefix) {
        this.prefix = prefix;
    }

    @Override
    public String get() {
        return prefix;
    }
}
