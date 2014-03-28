package com.netflix.suro.client;

import java.util.Map;

import javax.inject.Inject;

import com.google.inject.Provider;
import com.netflix.governator.guice.lazy.LazySingleton;
import com.netflix.loadbalancer.ILoadBalancer;
import com.netflix.suro.ClientConfig;

/**
 * Configuration based {@link ILoadBalancer} provider that will create an {@link ILoadBalancer} 
 * implementation based on the value of {@link ClienConfig.getLoadBalancerType()}.
 * 
 * To add a new ILoadBalancer type implementation see {@link SuroClientModule}
 * 
 * @author elandau
 *
 */
@LazySingleton
public class ConfigBasedLoadBalancerProvider implements Provider<ILoadBalancer> {
    private final Provider<ClientConfig> config;
    private final Map<String, Provider<ILoadBalancer>> impls;
    
    private static final String DEFAULT_LOAD_BALANCER_TYPE = "static";
    
    @Inject
    public ConfigBasedLoadBalancerProvider(Provider<ClientConfig> configProvider, Map<String, Provider<ILoadBalancer>> impls) {
        this.config = configProvider;
        this.impls  = impls;
    }
    
    @Override
    public ILoadBalancer get() {
        // Load the singleton ClientConfig lazily
        ClientConfig config = this.config.get();
        if (config.getLoadBalancerType() != null) {
            if (!impls.containsKey(config.getLoadBalancerType())) {
                throw new RuntimeException(
                        String.format("Unknown load balancer type '%s'.  Expecting one of %s", 
                            config.getLoadBalancerType(),
                            impls.keySet()));
            }
            return impls.get(config.getLoadBalancerType()).get();
        }
        else {
            return impls.get(DEFAULT_LOAD_BALANCER_TYPE).get();
        }
    }

}
