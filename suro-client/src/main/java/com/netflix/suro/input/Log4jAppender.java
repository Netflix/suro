package com.netflix.suro.input;

import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.netflix.governator.configuration.PropertiesConfigurationProvider;
import com.netflix.governator.guice.BootstrapBinder;
import com.netflix.governator.guice.BootstrapModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.loadbalancer.ILoadBalancer;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.client.SuroClient;
import com.netflix.suro.client.SyncSuroClient;
import com.netflix.suro.client.async.AsyncSuroClient;
import com.netflix.suro.client.async.FileBlockingQueue;
import com.netflix.suro.connection.EurekaLoadBalancer;
import com.netflix.suro.connection.StaticLoadBalancer;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.serde.MessageSerDe;
import com.netflix.suro.message.serde.SerDe;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Log4jAppender extends AppenderSkeleton {
    static final Logger log = LoggerFactory.getLogger(Log4jAppender.class);

    private String formatterClass = JsonLog4jFormatter.class.toString();
    public void setFormatterClass(String formatterClass) {
        this.formatterClass = formatterClass;
    }
    public String getFormatterClass() {
        return formatterClass;
    }

    private String app = "default";
    public void setApp(String app) {
        this.app = app;
    }
    public String getApp() {
        return app;
    }

    private String dataType = "default";
    public void setDataType(String dataType) {
        this.dataType = dataType;
    }
    private String getDataType() {
        return dataType;
    }

    private byte compression = 1;
    public void setCompression(byte compression) {
        this.compression = compression;
    }
    public byte getCompression() {
        return compression;
    }

    private String loadBalancerType = "eureka";
    public void setLoadBalancerType(String loadBalancerType) {
        this.loadBalancerType = loadBalancerType;
    }
    private String getLoadBalancerType() {
        return loadBalancerType;
    }

    private String loadBalancerServer;
    public void setLoadBalancerServer(String loadBalancerServer) {
        this.loadBalancerServer = loadBalancerServer;
    }
    private String getLoadBalancerServer() {
        return loadBalancerServer;
    }

    private String asyncQueueType = "memory";
    public void setAsyncQueueType(String asyncQueueType) {
        this.asyncQueueType = asyncQueueType;
    }
    public String getAsyncQueueType() {
        return asyncQueueType;
    }

    private String fileQueuePath = "/logs/suroClient";
    public String getFileQueuePath() {
        return fileQueuePath;
    }
    public void setFileQueuePath(String fileQueuePath) {
        this.fileQueuePath = fileQueuePath;
    }

    private String clientType = "async";
    public void setClientType(String clientType) {
        this.clientType = clientType;
    }
    public String getClientType() {
        return clientType;
    }

    private String routingKey = "default";
    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }
    public String getRoutingKey() {
        return routingKey;
    }

    private Injector injector;
    private Log4jFormatter formatter;
    private SuroClient client;

    @Override
    public void activateOptions() {
        final Properties properties = createProperties();

        ClientConfig config = createInjector(properties).getInstance(ClientConfig.class);

        client = injector.getInstance(SuroClient.class);
        try {
            formatter = (Log4jFormatter) Class.forName(formatterClass).newInstance();
        } catch (Exception e) {
            formatter = new JsonLog4jFormatter(config);
        }
    }

    private Injector createInjector(final Properties properties) {
        injector = LifecycleInjector
                .builder()
                .withBootstrapModule
                        (
                                new BootstrapModule() {
                                    @Override
                                    public void configure(BootstrapBinder binder) {
                                        binder.bindConfigurationProvider().toInstance(
                                                new PropertiesConfigurationProvider(properties));

                                        if (loadBalancerType.equals("eureka")) {
                                            binder.bind(ILoadBalancer.class).to(EurekaLoadBalancer.class);
                                        } else {
                                            binder.bind(ILoadBalancer.class).to(StaticLoadBalancer.class);
                                        }

                                        if (clientType.equals("async")) {
                                            binder.bind(SuroClient.class).to(AsyncSuroClient.class);
                                            if (asyncQueueType.equals("memory")) {
                                                binder.bind(new TypeLiteral<BlockingQueue<Message>>() {})
                                                        .to(new TypeLiteral<LinkedBlockingQueue<Message>>() {
                                                        });
                                            } else {
                                                binder.bind(new TypeLiteral<BlockingQueue<Message>>() {})
                                                        .to(new TypeLiteral<FileBlockingQueue<Message>>() {});
                                                binder.bind(new TypeLiteral<SerDe<Message>>(){}).to(new TypeLiteral<MessageSerDe>() {});
                                            }
                                        } else {
                                            binder.bind(SuroClient.class).to(SyncSuroClient.class);
                                        }
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

    private Properties createProperties() {
        Properties properties = new Properties();
        properties.setProperty(ClientConfig.APP, app);
        properties.setProperty(ClientConfig.DATA_TYPE, dataType);
        properties.setProperty(ClientConfig.COMPRESSION, Byte.toString(compression));
        properties.setProperty(ClientConfig.LB_TYPE, loadBalancerType);
        properties.setProperty(ClientConfig.LB_SERVER, loadBalancerServer);
        properties.setProperty(ClientConfig.ASYNC_QUEUE_TYPE, asyncQueueType);
        properties.setProperty(ClientConfig.ASYNC_FILEQUEUE_PATH, fileQueuePath);
        properties.setProperty(ClientConfig.CLIENT_TYPE, clientType);
        properties.setProperty(ClientConfig.ROUTING_KEY, routingKey);

        return properties;
    }

    @Override
    public void doAppend(LoggingEvent event) {
        this.append(event);
    }

    @Override
    protected void append(LoggingEvent event) {
        String result = formatter.format(event);
        client.send(new Message(
                formatter.getRoutingKey() == null ? routingKey : formatter.getRoutingKey(),
                result.getBytes()));
    }

    @Override
    public void close() {
        injector.getInstance(LifecycleManager.class).close();
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    public long getSentMessageCount() {
        return client.getSentMessageCount();
    }
}
