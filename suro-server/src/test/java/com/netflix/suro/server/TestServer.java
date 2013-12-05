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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.suro.SuroPlugin;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.routing.RoutingPlugin;
import com.netflix.suro.routing.TestMessageRouter;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

public class TestServer {
    private static ThriftServer server;

    public static Injector start() throws TTransportException {
        Injector injector = LifecycleInjector.builder()
            .withModules(
                new SuroPlugin() {
                    @Override
                    protected void configure() {
                        bind(ObjectMapper.class).to(DefaultObjectMapper.class);
                        this.addSinkType("TestSink", TestMessageRouter.TestSink.class);
                    }
                },
                new RoutingPlugin()
            ).createInjector();

        server = injector.getInstance(ThriftServer.class);
        server.start();

        return injector;
    }

    public static void shutdown() {
        server.shutdown();
    }

    @Test
    public void test() throws TTransportException {
        start();
        HealthCheck.checkConnection("localhost", 7101, 5000);
        shutdown();
    }
}
