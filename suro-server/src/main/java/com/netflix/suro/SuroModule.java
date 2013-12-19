/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.suro;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.netflix.suro.aws.PropertyAWSCredentialsProvider;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.routing.RoutingMap;
import com.netflix.suro.server.StatusServer;
import com.netflix.suro.sink.SinkManager;

import java.util.Properties;

/**
 * Guice module for binding {@link AWSCredentialsProvider},
 * Jackson {@link ObjectMapper}, {@link SinkManager}, {@link RoutingMap},
 * {@link SuroService}, {@link StatusServer}
 *
 * @author elandau
 */
public class SuroModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(AWSCredentialsProvider.class)
            .annotatedWith(Names.named("credentials")).to(PropertyAWSCredentialsProvider.class);

        bind(ObjectMapper.class).to(DefaultObjectMapper.class).asEagerSingleton();
        bind(AWSCredentialsProvider.class).to(PropertyAWSCredentialsProvider.class);
        bind(SuroService.class);
        bind(StatusServer.class);
    }
}
