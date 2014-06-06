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

import com.google.inject.AbstractModule;
import com.netflix.suro.input.DynamicPropertyInputConfigurator;
import com.netflix.suro.routing.DynamicPropertyRoutingMapConfigurator;
import com.netflix.suro.sink.DynamicPropertySinkConfigurator;

/**
 * Guice module for binding DynamicProperty based configuration of sink and routing map
 *
 * @author elandau
 */
public class SuroDynamicPropertyModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(DynamicPropertyInputConfigurator.class).asEagerSingleton();
        bind(DynamicPropertySinkConfigurator.class).asEagerSingleton();
        bind(DynamicPropertyRoutingMapConfigurator.class).asEagerSingleton();
    }
}
