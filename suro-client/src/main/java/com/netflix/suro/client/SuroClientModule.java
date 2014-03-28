package com.netflix.suro.client;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;
import com.netflix.governator.guice.lazy.LazySingletonScope;
import com.netflix.loadbalancer.ILoadBalancer;
import com.netflix.suro.client.async.AsyncSuroClient;
import com.netflix.suro.connection.EurekaLoadBalancer;
import com.netflix.suro.connection.StaticLoadBalancer;

@Singleton
public class SuroClientModule extends AbstractModule {
    @Override
    protected void configure() {
        MapBinder<String, ILoadBalancer> loadBalancers = MapBinder.newMapBinder(binder(), String.class, ILoadBalancer.class);
        loadBalancers.addBinding("eureka").to(EurekaLoadBalancer.class);
        loadBalancers.addBinding("static").to(StaticLoadBalancer.class);
        
        MapBinder<String, ISuroClient> clientImpls = MapBinder.newMapBinder(binder(), String.class, ISuroClient.class);
        clientImpls.addBinding("async").to(AsyncSuroClient.class).in(LazySingletonScope.get());
        clientImpls.addBinding("sync").to(SyncSuroClient.class).in(LazySingletonScope.get());
        
        bind(ISuroClient.class).toProvider(ConfigBasedSuroClientProvider.class);
        bind(ILoadBalancer.class).toProvider(ConfigBasedLoadBalancerProvider.class);
    }

}
