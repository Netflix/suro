package com.netflix.suro.client;

import java.util.Map;

import javax.inject.Inject;

import com.google.inject.Provider;
import com.netflix.governator.guice.lazy.LazySingleton;
import com.netflix.suro.ClientConfig;

/**
 * Configuration base {@link ISuroClient} provider that will create an {@link ISuroClient} 
 * implementation based on the value of {@link ClienConfig.getClientType()}.
 * 
 * To add a new client type implementation see {@link SuroClientModule}
 * 
 * @author elandau
 *
 */
@LazySingleton
public class ConfigBasedSuroClientProvider implements Provider<ISuroClient> {
    private final Provider<ClientConfig> configProvider;
    private final Map<String, Provider<ISuroClient>> clientImpls;
    
    private static final String DEFAULT_CLIENT_TYPE = "sync";
    
    @Inject
    public ConfigBasedSuroClientProvider(Provider<ClientConfig> configProvider, Map<String, Provider<ISuroClient>> clientImpls) {
        this.configProvider = configProvider;
        this.clientImpls    = clientImpls;
    }
    
    @Override
    public ISuroClient get() {
        // Load the singleton ClientConfig lazily
        ClientConfig config = configProvider.get();
        if (config.getClientType() != null) {
            if (!clientImpls.containsKey(config.getClientType())) {
                throw new RuntimeException(
                        String.format("Unknown client type '%s'.  Expecting one of %s", 
                            config.getClientType(),
                            clientImpls.keySet()));
            }
            return clientImpls.get(config.getClientType()).get();
        }
        else {
            return clientImpls.get(DEFAULT_CLIENT_TYPE).get();
        }
        
    }

}
