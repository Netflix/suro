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

import com.google.common.io.Closeables;
import com.google.inject.Injector;
import com.netflix.governator.configuration.PropertiesConfigurationProvider;
import com.netflix.governator.guice.BootstrapBinder;
import com.netflix.governator.guice.BootstrapModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.suro.server.StatusServer;
import org.apache.commons.cli.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Command line driver for Suro
 * 
 * @author metacret
 * @author elandau
 */
public class SuroServer {
    private static final String PROP_PREFIX = "SuroServer.";
    
    public static void main(String[] args) throws IOException {
        LifecycleManager manager = null;

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
                properties.setProperty(propName, value);
            }

            // Create the injector
            Injector injector = LifecycleInjector.builder()
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
                        new SuroModule(properties),
                        new SuroFastPropertyModule(),
                        StatusServer.createJerseyServletModule()
                     )
                    .createInjector();

            manager = injector.getInstance(LifecycleManager.class);
            manager.start();
            
            // Hmmm... is this the right way to do this?  Should this block on the status server instead?
            Thread.sleep(Long.MAX_VALUE);
            
        } catch (Exception e) {
            System.err.println("SuroServer startup failed: " + e.getMessage());
            System.exit(-1);
        } finally {
            Closeables.close(manager, true);
        }
    }
    
    @SuppressWarnings("static-access")
    private static Options createOptions() {
        Option propertyFile = OptionBuilder.withArgName("serverProperty")
                .hasArg()
                .isRequired(true)
                .withDescription("server property file path")
                .create('p');

        Option mapFile = OptionBuilder.withArgName("messageMap")
                .hasArg()
                .isRequired(true)
                .withDescription("message routing map file path")
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
}
