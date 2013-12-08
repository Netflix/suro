package com.netflix.suro.routing;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.suro.message.MessageContainer;

/**
 * Converts an {@link com.netflix.suro.message.MessageContainer} to a Java object that is suitable
 * for filtering by {@link XPathFilter}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = JsonMapConverter.TYPE, value = JsonMapConverter.class),
})
public interface MessageConverter<T> {
    /**
     * Converts the message in the given message container to an object of type T
     * @param message The message container that contains the message to be converted.
     *
     * @return Converted object from the given message container
     */
    public T convert(MessageContainer message);
}
