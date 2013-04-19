package com.netflix.suro.server;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.GuiceServletContextListener;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class StatusServer {
    static Logger log = LoggerFactory.getLogger(StatusServer.class);

    public static JerseyServletModule createJerseyServletModule() {
        return new JerseyServletModule() {
            @Override
            protected void configureServlets() {
                bind(HealthCheck.class);
                bind(SinkStat.class);
                bind(GuiceContainer.class).asEagerSingleton();
                serve("/*").with(GuiceContainer.class);
            }
        };
    }

    private final Server server;
    @Inject
    public StatusServer(ServerConfig config) {
        log.info("StatusServer started on the port: " + config.getStatusServerPort());
        server = new Server(config.getStatusServerPort());
    }

    public void start(final Injector injector) {
        ServletContextHandler sch = new ServletContextHandler(server, "/");
        sch.addEventListener(new GuiceServletContextListener() {
            @Override
            protected Injector getInjector() {
                return injector;
            }
        });
        sch.addFilter(GuiceFilter.class, "/*", null);
        sch.addServlet(DefaultServlet.class, "/");

        try {
            server.start();
        } catch (Exception e) {
            log.error("Exception on StatusServer starting: " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        try {
            log.info("shutting down StatusServer");
            server.stop();
        } catch (Exception e) {
            //ignore exceptions while shutdown
        }
    }
}
