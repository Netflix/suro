package com.netflix.suro.sink;

/**
 * Holder for a Sink subtype to be registered with guice as a Map<String, SinkType>
 * 
 * Tried to use TypeLiteral but guice doesn't seem to like mixing TypeLiteral with
 * MapBinder
 * 
 * @author elandau
 *
 */
public class SinkType {
    private Class<?> type;
    
    public SinkType(Class<? extends Sink> type) {
        this.type = type;
    }
    
    public Class<?> getRawType() {
        return type;
    }
}
