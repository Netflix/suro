package com.netflix.suro;

import org.apache.log4j.Logger;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.SinkType;

/**
 * Guice based suro plugin with convenience methods for adding pluggable components
 * using Guice's MapBinder.
 * 
 * @author elandau
 *
 */
public abstract class SuroPlugin extends AbstractModule {
    static final Logger LOG = Logger.getLogger(SuroServer.class);
    
    /**
     * Add a sink implementation to Suro.  typeName is the expected value of the
     * 'type' field of a JSON configuration.
     * 
     * @param typeName
     * @param sinkClass
     */
    public <T extends Sink> void addSinkType(String typeName, Class<T> sinkClass) {
        LOG.info("Adding sinkType : " + typeName + " -> " + sinkClass.getCanonicalName());
        
        MapBinder<String, SinkType> consumers 
            = MapBinder.newMapBinder(binder(), String.class, SinkType.class);

        consumers.addBinding(typeName).toInstance(new SinkType(sinkClass));
    }
}
