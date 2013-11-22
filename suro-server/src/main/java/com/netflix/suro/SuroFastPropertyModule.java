package com.netflix.suro;

import com.google.inject.AbstractModule;
import com.netflix.suro.routing.FastPropertyRoutingMapConfigurator;
import com.netflix.suro.sink.FastPropertySinkConfigurator;

/**
 * Guice module for binding FastProperty based configuration of sink and routing map
 *
 * @author elandau
 */
public class SuroFastPropertyModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(FastPropertySinkConfigurator.class).asEagerSingleton();
        bind(FastPropertyRoutingMapConfigurator.class).asEagerSingleton();
    }
}
