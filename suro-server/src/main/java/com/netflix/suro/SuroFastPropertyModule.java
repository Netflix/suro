package com.netflix.suro;

import com.google.inject.AbstractModule;
import com.netflix.suro.routing.DynamicPropertyRoutingMapConfigurator;
import com.netflix.suro.sink.DynamicPropertySinkConfigurator;

/**
 * Guice module for binding FastProperty based configuration of sink and routing map
 *
 * @author elandau
 */
public class SuroFastPropertyModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(DynamicPropertySinkConfigurator.class).asEagerSingleton();
        bind(DynamicPropertyRoutingMapConfigurator.class).asEagerSingleton();
    }
}
