package com.netflix.suro.sink.elasticsearch;

import com.google.inject.multibindings.Multibinder;
import com.netflix.suro.SuroPlugin;
import com.netflix.suro.TypeHolder;

public class ElasticSearchPlugin extends SuroPlugin {
    @Override
    protected void configure() {
        LOG.info("Adding IndexInfoBuilder: " + "default" + "->" + DefaultIndexInfoBuilder.class.getCanonicalName());

        Multibinder<TypeHolder> bindings
                = Multibinder.newSetBinder(binder(), TypeHolder.class);
        bindings.addBinding().toInstance(new TypeHolder("default", DefaultIndexInfoBuilder.class));

        addSinkType(ElasticSearchSink.TYPE, ElasticSearchSink.class);
    }
}
