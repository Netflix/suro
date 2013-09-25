package com.netflix.suro;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.netflix.suro.aws.PropertyAWSCredentialsProvider;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.serde.SerDe;
import com.netflix.suro.queue.MessageSetSerDe;
import com.netflix.suro.routing.FastPropertyRoutingMapConfigurator;
import com.netflix.suro.routing.RoutingMap;
import com.netflix.suro.server.ServerConfig;
import com.netflix.suro.server.StatusServer;
import com.netflix.suro.sink.FastPropertySinkConfigurator;
import com.netflix.suro.sink.SinkManager;
import com.netflix.suro.thrift.TMessageSet;

public class SuroModule extends AbstractModule {
    private final Properties properties;
    
    public SuroModule(Properties properties) {
        this.properties = properties;
    }
    
    @Override
    protected void configure() {
        // TODO: Move this into a provider
        if (properties.getProperty(ServerConfig.QUEUE_TYPE, "memory").equals("memory")) {
            bind(new TypeLiteral<BlockingQueue<TMessageSet>>() {})
            .toProvider(new Provider<LinkedBlockingQueue<TMessageSet>>() {
                @Override
                public LinkedBlockingQueue<TMessageSet> get() {
                    return new LinkedBlockingQueue<TMessageSet>(
                            Integer.parseInt(
                                    properties.getProperty(
                                            ServerConfig.MEMORY_QUEUE_SIZE, "100"))
                            );
                }
            });
        } 
        else {
            bind(new TypeLiteral<BlockingQueue<TMessageSet>>() {})
                  .to(new TypeLiteral<FileBlockingQueue<TMessageSet>>() {});
            bind(new TypeLiteral<SerDe<TMessageSet>>() {})
                  .to(new TypeLiteral<MessageSetSerDe>() {});
        }
        
        bind(AWSCredentialsProvider.class)
            .annotatedWith(Names.named("credentials")).to(PropertyAWSCredentialsProvider.class);

        bind(ObjectMapper.class).to(DefaultObjectMapper.class);
        bind(AWSCredentialsProvider.class).to(PropertyAWSCredentialsProvider.class);
        bind(SinkManager.class);
        bind(RoutingMap.class);
        bind(FastPropertyRoutingMapConfigurator.class);
        bind(FastPropertySinkConfigurator.class);
        bind(SuroService.class);
        bind(StatusServer.class);
    }
}
