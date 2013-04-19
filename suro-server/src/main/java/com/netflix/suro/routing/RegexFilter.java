package com.netflix.suro.routing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.serde.SerDe;

import java.util.regex.Pattern;

public class RegexFilter implements Filter {
    private final Pattern filterPattern;

    @JsonCreator
    public RegexFilter(@JsonProperty("regex") String regex) {
        filterPattern = Pattern.compile(regex);
    }

    @Override
    public boolean doFilter(Message msg, SerDe serde) {
        String strMsg = serde.toString(msg.getPayload());
        return filterPattern.matcher(strMsg).find();
    }
}
