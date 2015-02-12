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

package com.netflix.suro;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.netflix.governator.configuration.PropertiesConfigurationProvider;
import com.netflix.governator.guice.BootstrapBinder;
import com.netflix.governator.guice.BootstrapModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.suro.input.DynamicPropertyInputConfigurator;
import com.netflix.suro.input.SuroInputPlugin;
import com.netflix.suro.routing.DynamicPropertyRoutingMapConfigurator;
import com.netflix.suro.routing.RoutingPlugin;
import com.netflix.suro.server.StatusServer;
import com.netflix.suro.sink.DynamicPropertySinkConfigurator;
import com.netflix.suro.sink.ServerSinkPlugin;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Command line driver for Suro
 * 
 * @author jbae
 * @author elandau
 */
public class SuroServer {
    private static final String PROP_PREFIX = "SuroServer.";
    private static final int DEFAULT_CONTROL_PORT = 9090;
    public static final String OPT_CONTROL_PORT = "controlPort";

    public static void main(String[] args) throws IOException {
        final AtomicReference<Injector> injector = new AtomicReference<Injector>();

        try {
            // Parse the command line
            Options           options = createOptions();
            final CommandLine line    = new BasicParser().parse(options, args);
            
            // Load the properties file
            final Properties properties = new Properties();
            if (line.hasOption('p')) {
                properties.load(new FileInputStream(line.getOptionValue('p')));
            }

            // Bind all command line options to the properties with prefix "SuroServer."
            for (Option opt : line.getOptions()) {
                String name     = opt.getOpt();
                String value    = line.getOptionValue(name);
                String propName = PROP_PREFIX + opt.getArgName();
                if (propName.equals(DynamicPropertyRoutingMapConfigurator.ROUTING_MAP_PROPERTY)) {
                    properties.setProperty(DynamicPropertyRoutingMapConfigurator.ROUTING_MAP_PROPERTY,
                            FileUtils.readFileToString(new File(value)));
                } else if (propName.equals(DynamicPropertySinkConfigurator.SINK_PROPERTY)) {
                    properties.setProperty(DynamicPropertySinkConfigurator.SINK_PROPERTY,
                            FileUtils.readFileToString(new File(value)));
                } else if (propName.equals(DynamicPropertyInputConfigurator.INPUT_CONFIG_PROPERTY)) {
                    properties.setProperty(DynamicPropertyInputConfigurator.INPUT_CONFIG_PROPERTY,
                            FileUtils.readFileToString(new File(value)));
                } else {
                    properties.setProperty(propName, value);
                }
            }

            List<Module> extensionModules = null;
            if (line.hasOption('x')) {
                String moduleFile = line.getOptionValue('x');
                List<String> extensionModuleClasses = new ObjectMapper().readValue(
                        FileUtils.readFileToString(new File(moduleFile)),
                        new TypeReference<List<String>>(){});

                if(extensionModuleClasses != null){
                    extensionModules = Lists.transform(extensionModuleClasses, new Function<String, Module>() {
                        @Nullable
                        @Override
                        public Module apply(String input) {
                            try {
                                return (Module)Class.forName(input).newInstance();
                            } catch (Throwable e) {
                                throw new RuntimeException(String.format("Unable to load module class %s", input), e);
                            }
                        }
                    });
                }
            }

            if(extensionModules == null) { //catch-all for either no configuration or empty configuration file
                extensionModules = Lists.newArrayList();
            }

            create(injector, properties, extensionModules.toArray(new Module[extensionModules.size()]));

            injector.get().getInstance(LifecycleManager.class).start();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        Closeables.close(injector.get().getInstance(LifecycleManager.class), true);
                    } catch (IOException e) {
                        // do nothing because Closeables.close will swallow IOException
                    }
                }
            });

            waitForShutdown(getControlPort(options));
        } catch (Throwable e) {
            System.err.println("SuroServer startup failed: " + e.getMessage());
            System.exit(-1);
        } finally {
            Closeables.close(injector.get().getInstance(LifecycleManager.class), true);
        }
    }

    public static void create(AtomicReference<Injector> injector, final Properties properties, Module... modules) throws Exception {
        // Create the injector
        injector.set(LifecycleInjector.builder()
                .withBootstrapModule(
                        new BootstrapModule() {
                            @Override
                            public void configure(BootstrapBinder binder) {
                                binder.bindConfigurationProvider().toInstance(
                                        new PropertiesConfigurationProvider(properties));
                            }
                        }
                )
                .withModules(
                        new RoutingPlugin(),
                        new ServerSinkPlugin(),
                        new SuroInputPlugin(),
                        new SuroDynamicPropertyModule(),
                        new SuroModule(),
                        StatusServer.createJerseyServletModule()
                )
                .withAdditionalModules(modules)
                .build().createInjector());
    }

    private static void waitForShutdown(int port) throws IOException {
       new SuroControl().start(port);
    }

    private static int getControlPort(Options options) {
        Option opt = options.getOption("c");
        String value = opt.getValue();
        if(value == null) {
            return DEFAULT_CONTROL_PORT;
        }

        return Integer.parseInt(value);
    }

    @SuppressWarnings("static-access")
    private static Options createOptions() {
        Option propertyFile = OptionBuilder.withArgName("serverProperty")
                .hasArg()
                .withDescription("server property file path")
                .create('p');

        Option mapFile = OptionBuilder.withArgName("routingMap")
                .hasArg()
                .isRequired(true)
                .withDescription("message routing map file path")
                .create('m');

        Option sinkFile = OptionBuilder.withArgName("sinkConfig" )
                .hasArg()
                .isRequired(true)
                .withDescription("sink")
                .create('s');

        Option inputFile = OptionBuilder.withArgName("inputConfig" )
                .hasArg()
                .isRequired(true)
                .withDescription("input")
                .create('i');

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

        Option controlPort = OptionBuilder.withArgName(OPT_CONTROL_PORT)
                .hasArg()
                .isRequired(false)
                .withDescription("The port used to send command to this server")
                .create('c');

        Option extensions = OptionBuilder.withArgName("extensions")
                .hasArg()
                .isRequired(false)
                .withDescription("extension module list configuration file")
                .create('x');

        Options options = new Options();
        options.addOption(propertyFile);
        options.addOption(mapFile);
        options.addOption(sinkFile);
        options.addOption(inputFile);
        options.addOption(accessKey);
        options.addOption(secretKey);
        options.addOption(controlPort);
        options.addOption(extensions);

        return options;
    }
}
