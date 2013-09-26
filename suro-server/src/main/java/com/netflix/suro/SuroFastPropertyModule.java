package com.netflix.suro;

import com.google.inject.AbstractModule;
import com.netflix.suro.routing.FastPropertyRoutingMapConfigurator;
import com.netflix.suro.sink.FastPropertySinkConfigurator;

public class SuroFastPropertyModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(FastPropertyRoutingMapConfigurator.class);
        bind(FastPropertySinkConfigurator.class);
    }
}
