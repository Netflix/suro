package com.netflix.suro.sink.remotefile;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = StaticPrefixFormatter.TYPE, value = StaticPrefixFormatter.class),
        @JsonSubTypes.Type(name = DateRegionStackFormatter.TYPE, value = DateRegionStackFormatter.class)
})
public interface RemotePrefixFormatter {
    String get();
}
