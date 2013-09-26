/*
 * Copyright 2013 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.suro.server;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

@Singleton
public class StatusServer {
    static Logger log = LoggerFactory.getLogger(StatusServer.class);

    public static ServletModule createJerseyServletModule() {
        return new ServletModule() {
            @Override
            protected void configureServlets() {
                bind(HealthCheck.class);
                bind(SinkStat.class);
                bind(GuiceContainer.class).asEagerSingleton();
                serve("/*").with(GuiceContainer.class);
            }
        };
    }

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ServerConfig    config;
    private final Injector        injector;

    @Inject
    public StatusServer(ServerConfig config, Injector injector) {
        this.injector = injector;
        this.config = config;
    }

    public void start() {
        log.info("StatusServer starting");
        executor.submit(new Runnable() {
            @Override
            public void run() {
                Server server = new Server(config.getStatusServerPort());
                 
                log.info("Starting status server on " + config.getStatusServerPort());
                
                // Create a servlet context and add the jersey servlet.
                ServletContextHandler sch = new ServletContextHandler(server, "/");
                 
                // Add our Guice listener that includes our bindings
                sch.addEventListener(new GuiceServletContextListener() {
                    @Override
                    protected Injector getInjector() {
                        return injector;
                    }
                });
                 
                // Then add GuiceFilter and configure the server to 
                // reroute all requests through this filter. 
                sch.addFilter(GuiceFilter.class, "/*", null);
                 
                // Must add DefaultServlet for embedded Jetty. 
                // Failing to do this will cause 404 errors.
                // This is not needed if web.xml is used instead.
                sch.addServlet(DefaultServlet.class, "/");
                 
                // Start the server
                try {
                    server.start();
                    server.join();
                } catch (Exception e) {
                    log.error("Failed to start status server", e);
                } finally {
                    try {
                        server.stop();
                    } catch (Exception e) {
                        log.error("Failed to join status server", e);
                    }
                }
            }
        });
    }

    public void shutdown() {
        try {
            log.info("StatusServer shutting down");
            executor.shutdownNow();
        } catch (Exception e) {
            //ignore exceptions while shutdown
            log.error("Exception while shutting down: " + e.getMessage(), e);
        }
    }
}
