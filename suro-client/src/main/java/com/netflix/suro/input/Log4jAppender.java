package com.netflix.suro.input;

import com.google.inject.Injector;
import com.netflix.governator.configuration.PropertiesConfigurationProvider;
import com.netflix.governator.guice.BootstrapBinder;
import com.netflix.governator.guice.BootstrapModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.suro.ClientConfig;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.Loader;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Properties;

public class Log4jAppender extends AppenderSkeleton {
    private int connectionTimeout;
    private void setConnectionTimeout(int timeout) {
        this.connectionTimeout = timeout;
    }
    private int getConnectionTimeout() {
        return connectionTimeout;
    }
    private String formatterClass;
    private void setFormatter(String formatterClass) {
        this.formatterClass = formatterClass;
    }
    private String getFormatter() {
        return formatterClass;
    }

    private Injector injector;
    private Log4jFormatter formatter;
    @Override
    public void activateOptions() {
        final Properties properties = createProperties();

        ClientConfig config = createInjector(properties).getInstance(ClientConfig.class);

        createFormatter(config);
    }

    private void createFormatter(ClientConfig config) {
        try {
            formatter = (Log4jFormatter)
                    Loader.loadClass(config.getLog4jFormatter())
                            .getConstructor(ClientConfig.class)
                            .newInstance(config);

            if (formatter == null) {
                throw new RuntimeException("invalid formatter class name");
            }
        } catch (Exception e) {
            LogLog.error("Could not instantiate formatter: " + config.getLog4jFormatter(), e);
        }

        formatter = new StringLog4jFormatter(config);
    }

    private Injector createInjector(final Properties properties) {
        injector = LifecycleInjector
                .builder()
                .withBootstrapModule
                        (
                                new BootstrapModule()
                                {
                                    @Override
                                    public void configure(BootstrapBinder binder)
                                    {
                                        // bind the configuration provider
                                        binder.bindConfigurationProvider().toInstance(
                                                new PropertiesConfigurationProvider(properties));
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
        final Properties properties = new Properties();
        properties.put(ClientConfig.CONNECTION_TIMEOUT, connectionTimeout);
        properties.put(ClientConfig.LOG4J_FORMATTER, formatterClass);
        return properties;
    }

    @Override
    public void doAppend(LoggingEvent event) {
        this.append(event);
    }

    @Override
    protected void append(LoggingEvent event) {
        String result = formatter.format(event);
    }

    @Override
    public void close() {
        injector.getInstance(LifecycleManager.class).close();
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }
}
