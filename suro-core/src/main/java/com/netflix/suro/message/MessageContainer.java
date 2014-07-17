package com.netflix.suro.message;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Wrapper for a message with caching for deserialization
 * 
 * @author elandau
 */
public interface MessageContainer {
    /**
     * Get the 'routing' key for this message.  Note that the routing key is not 
     * part of the message and does not require deserialization.
     * @return
     */
    public String getRoutingKey();
    
    /**
     * Deserialize the message body into the requested entity.  
     * @param clazz
     * @return
     * @throws Exception
     */
    public <T> T getEntity(Class<T> clazz) throws Exception;

    public <T> T getEntity(TypeReference<T> typeReference) throws Exception;
    
    /**
     * Return the raw message
     * @return
     */
    public Message getMessage();
}
