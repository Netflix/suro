package com.netflix.suro.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.inject.Injector;
import com.netflix.governator.guice.BootstrapBinder;
import com.netflix.governator.guice.BootstrapModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.suro.message.serde.DefaultSerDeFactory;
import com.netflix.suro.message.serde.SerDeFactory;
import com.netflix.suro.queue.MemoryMessageQueue;
import com.netflix.suro.queue.MessageQueue;
import com.netflix.suro.routing.TestMessageRouter;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestServer {
    private static ThriftServer server;

    @BeforeClass
    public static Injector start() throws TTransportException {
        Injector injector = LifecycleInjector.builder().withBootstrapModule(new BootstrapModule() {
            @Override
            public void configure(BootstrapBinder binder) {
                binder.bind(MessageQueue.class).to(MemoryMessageQueue.class);
                binder.bind(SerDeFactory.class).to(DefaultSerDeFactory.class);

                ObjectMapper jsonMapper = new ObjectMapper();
                jsonMapper.registerSubtypes(new NamedType(TestMessageRouter.TestSink.class, "TestSink"));
                binder.bind(ObjectMapper.class).toInstance(jsonMapper);
            }
        }).createInjector();

        server = injector.getInstance(ThriftServer.class);
        server.start();

        return injector;
    }

    @AfterClass
    public static void shutdown() {
        server.shutdown();
    }

    @Test
    public void test() {
        HealthCheck.checkConnection("localhost", 7101, 5000);
    }
}
