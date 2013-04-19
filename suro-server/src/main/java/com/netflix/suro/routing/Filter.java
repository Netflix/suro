package com.netflix.suro.routing;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.serde.SerDe;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "regex", value = RegexFilter.class)
})
public interface Filter {
    public boolean doFilter(Message msg, SerDe serde);
}