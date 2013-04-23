package com.netflix.suro;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.netflix.governator.configuration.PropertiesConfigurationProvider;
import com.netflix.governator.guice.BootstrapBinder;
import com.netflix.governator.guice.BootstrapModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.suro.client.async.FileBlockingQueue;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.serde.DefaultSerDeFactory;
import com.netflix.suro.message.serde.SerDe;
import com.netflix.suro.message.serde.SerDeFactory;
import com.netflix.suro.queue.MessageSetSerDe;
import com.netflix.suro.routing.MessageRouter;
import com.netflix.suro.routing.RoutingMap;
import com.netflix.suro.server.ServerConfig;
import com.netflix.suro.server.StatusServer;
import com.netflix.suro.server.ThriftServer;
import com.netflix.suro.sink.SinkManager;
import com.netflix.suro.thrift.TMessageSet;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class SuroServer {
    static Logger log = Logger.getLogger(SuroServer.class);

    private final ExecutorService statusServerExecutor = Executors.newSingleThreadExecutor();
    private StatusServer statusServer;
    private ThriftServer server;

    private final Properties properties;
    private final String mapDesc;
    private final String sinkDesc;

    private ObjectMapper jsonMapper;
    private AWSCredentialsProvider credentialsProvider;
    private Class<? extends SerDeFactory> serDeFactoryClass;

    private SuroServer(
            Properties properties,
            String mapDesc,
            String sinkDesc) {
        Preconditions.checkNotNull(properties);
        Preconditions.checkNotNull(mapDesc);
        Preconditions.checkNotNull(sinkDesc);

        this.properties = properties;
        this.mapDesc = mapDesc;
        this.sinkDesc = sinkDesc;
    }

    public void start() {
        createInjector(properties, credentialsProvider);

        statusServer = injector.getInstance(StatusServer.class);
        server = injector.getInstance(ThriftServer.class);

        try {
            startSinkManager(sinkDesc);
            startMessageRouter(mapDesc);
            startServer();
        } catch (Exception e) {
            log.error("Exception while starting up server: " + e.getMessage(), e);
            System.exit(-1);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                shutdown();
            }
        });
    }

    public void shutdown() {
        try {
            server.shutdown();
            statusServer.shutdown();
            injector.getInstance(MessageRouter.class).shutdown();
            injector.getInstance(SinkManager.class).shutdown();
            injector.getInstance(LifecycleManager.class).close();
        } catch (Exception e) {
            //ignore every exception while shutting down but loggign should be done for debugging
            log.error("Exception while shutting down SuroServer: " + e.getMessage(), e);
        }
    }

    private void startServer() throws TTransportException {
        server.start();
        statusServerExecutor.execute(new Runnable() {
            @Override
            public void run() {
                statusServer.start(injector);
            }
        });
    }

    private void startSinkManager(String sinkDesc) {
        SinkManager sinkManager = injector.getInstance(SinkManager.class);
        sinkManager.build(sinkDesc);
    }

    private void startMessageRouter(String mapDesc) {
        RoutingMap routingMap = injector.getInstance(RoutingMap.class);
        routingMap.build(mapDesc);
        MessageRouter router = injector.getInstance(MessageRouter.class);
        router.start();
    }

    private Injector injector;

    private Injector createInjector(
            final Properties properties,
            final AWSCredentialsProvider credentialsProvider) {
        injector = LifecycleInjector
                .builder()
                .withBootstrapModule
                        (
                                new BootstrapModule() {
                                    @Override
                                    public void configure(BootstrapBinder binder) {
                                        binder.bindConfigurationProvider().toInstance(
                                                new PropertiesConfigurationProvider(properties));

                                        if (properties.getProperty(ServerConfig.QUEUE_TYPE, "memory").equals("memory")) {
                                            binder.bind(new TypeLiteral<BlockingQueue<TMessageSet>>() {
                                            })
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
                                        } else {
                                            binder.bind(new TypeLiteral<BlockingQueue<TMessageSet>>() {
                                            })
                                                    .to(new TypeLiteral<FileBlockingQueue<TMessageSet>>() {
                                                    });
                                            binder.bind(new TypeLiteral<SerDe<TMessageSet>>() {
                                            })
                                                    .to(new TypeLiteral<MessageSetSerDe>() {
                                                    });
                                        }

                                        if (credentialsProvider != null) {
                                            final Map<String, Object> injectables = new ImmutableMap.Builder<String, Object>()
                                                    .put("credentials", credentialsProvider)
                                                    .build();
                                            jsonMapper.setInjectableValues(new InjectableValues() {
                                                @Override
                                                public Object findInjectableValue(
                                                        Object valueId,
                                                        DeserializationContext ctxt,
                                                        BeanProperty forProperty,
                                                        Object beanInstance) {
                                                    return injectables.get(valueId);
                                                }
                                            });
                                        }
                                        binder.bind(ObjectMapper.class).toInstance(jsonMapper);
                                        binder.bind(SerDeFactory.class).to(serDeFactoryClass);
                                    }
                                }
                        )
                .createInjector();
        LifecycleManager manager = injector.getInstance(LifecycleManager.class);

        try {
            manager.start();
        } catch (Exception e) {
            throw new RuntimeException("LifecycleManager cannot start with an exception: " + e.getMessage());
        }
        return injector;
    }

    public static class Builder {
        private Properties properties;
        private String mapDesc;
        private String sinkDesc;
        private AWSCredentialsProvider credentialsProvider;
        private ObjectMapper jsonMapper;
        private Class<? extends SerDeFactory> serDeFactoryclass;

        public Builder withProperties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public Builder withMessageRoutingMap(String mapDesc) {
            this.mapDesc = mapDesc;
            return this;
        }

        public Builder withSinkDescription(String sinkDesc) {
            this.sinkDesc = sinkDesc;
            return this;
        }

        public Builder withAWSCredentialsProvider(AWSCredentialsProvider provider) {
            this.credentialsProvider = provider;
            return this;
        }

        public Builder withJsonMapper(ObjectMapper jsonMapper) {
            this.jsonMapper = jsonMapper;
            return this;
        }

        public Builder withSerDeFactoryClass(Class<? extends SerDeFactory> serDeFactoryClass) {
            this.serDeFactoryclass = serDeFactoryClass;
            return this;
        }

        public SuroServer build() {
            SuroServer server = new SuroServer(properties, mapDesc, sinkDesc);
            server.jsonMapper = jsonMapper == null ? new DefaultObjectMapper() : jsonMapper;
            server.credentialsProvider = credentialsProvider;
            server.serDeFactoryClass = serDeFactoryclass == null ? DefaultSerDeFactory.class : serDeFactoryclass;

            return server;
        }
    }

    public static void main(String[] args) throws IOException {
        Options options = createOptions();

        Properties properties = new Properties();
        String mapDesc = null;
        String sinkDesc = null;

        CommandLineParser parser = new BasicParser();
        try {
            CommandLine line = parser.parse(options, args);
            properties.load(new FileInputStream(line.getOptionValue('p')));
            mapDesc = FileUtils.readFileToString(new File(line.getOptionValue('m')));
            sinkDesc = FileUtils.readFileToString(new File(line.getOptionValue('s')));
            SuroServer server = new SuroServer.Builder()
                    .withProperties(properties)
                    .withMessageRoutingMap(mapDesc)
                    .withSinkDescription(sinkDesc)
                    .withAWSCredentialsProvider(
                            createAWSCredentialProvider(line.getOptionValue('a'), line.getOptionValue('k')))
                    .build();
            server.start();
        } catch (Exception e) {
            System.err.println("SuroServer startup failed: " + e.getMessage());
            System.exit(-1);
        }


    }

    private static Options createOptions() {
        Option propertyFile = OptionBuilder.withArgName("serverProperty")
                .hasArg()
                .isRequired(true)
                .withDescription("server property file path")
                .create('p');

        Option mapFile = OptionBuilder.withArgName("messageMap")
                .hasArg()
                .isRequired(true)
                .withDescription(" message routing map file path")
                .create('m');

        Option sinkFile = OptionBuilder.withArgName("sink" )
                .hasArg()
                .isRequired(true)
                .withDescription("sink")
                .create('s');

        Option accessKey = OptionBuilder.withArgName("AWSAccessKey" )
                .hasArg()
                .isRequired(false)
                .withDescription("AWSAccessKey")
                .create('a');

        Option secretKey = OptionBuilder.withArgName("AWSSecretKey" )
                .hasArg()
                .isRequired(false)
                .withDescription("AWSSecretKey")
                .create('k');

        Options options = new Options();
        options.addOption(propertyFile);
        options.addOption(mapFile);
        options.addOption(sinkFile);
        options.addOption(accessKey);
        options.addOption(secretKey);
        return options;
    }

    private static AWSCredentialsProvider createAWSCredentialProvider(final String accessKey, final String secretKey) {
        if (accessKey != null && secretKey != null) {
            return new AWSCredentialsProvider() {
                @Override
                public AWSCredentials getCredentials() {
                    return new AWSCredentials() {
                        @Override
                        public String getAWSAccessKeyId() {
                            return accessKey;
                        }

                        @Override
                        public String getAWSSecretKey() {
                            return secretKey;
                        }
                    };
                }

                @Override
                public void refresh() {}
            };
        } else {
            // it should be injected separately
            return null;
        }
    }
}
