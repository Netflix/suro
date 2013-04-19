package com.netflix.suro;

import com.google.inject.Injector;
import com.netflix.governator.configuration.PropertiesConfigurationProvider;
import com.netflix.governator.guice.BootstrapBinder;
import com.netflix.governator.guice.BootstrapModule;
import com.netflix.governator.guice.LifecycleInjector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.loadbalancer.ILoadBalancer;
import com.netflix.suro.client.SyncSuroClient;
import com.netflix.suro.client.async.AsyncSuroClient;
import com.netflix.suro.connection.StaticLoadBalancer;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.serde.SerDe;
import com.netflix.suro.message.serde.StringSerDe;
import com.netflix.suro.routing.TestMessageRouter;
import com.netflix.suro.server.TestServer;
import com.netflix.suro.sink.SinkManager;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class TestSuroClient {
    private Injector serverInjector;
    private Injector clientInjector;
    private SinkManager sinkManager;
    private LifecycleManager manager;

    @Before
    public void createServerClient() throws Exception {
        // create the test server
        serverInjector = TestServer.start();
        sinkManager = TestMessageRouter.startSinkMakager(serverInjector);
        TestMessageRouter.startMessageRouter(serverInjector);

        // create the client
        final Properties clientProperties = new Properties();
        clientProperties.setProperty(ClientConfig.APP, "testSuroClient");
        clientProperties.setProperty(ClientConfig.DATA_TYPE, "testSuroDataType");
        clientProperties.setProperty(ClientConfig.LB_SERVER, "localhost:7101");

        clientInjector = LifecycleInjector.builder().withBootstrapModule(new BootstrapModule() {
            @Override
            public void configure(BootstrapBinder binder) {
                binder.bindConfigurationProvider().toInstance(new PropertiesConfigurationProvider(clientProperties));

                binder.bind(ILoadBalancer.class).to(StaticLoadBalancer.class);
                binder.bind(SerDe.class).to(StringSerDe.class);
            }
        }).createInjector();
        manager = clientInjector.getInstance(LifecycleManager.class);
        manager.start();
    }

    @After
    public void shutdown() {
        manager.close();
        TestServer.shutdown();
    }

    @Test
    public void testSyncClient() throws TTransportException {
        SyncSuroClient client = clientInjector.getInstance(SyncSuroClient.class);

        // send the message
        client.send(new Message("routingKey", "testMessage".getBytes()));

        // check the test server whether it got received
        TestMessageRouter.TestSink testSink = (TestMessageRouter.TestSink) sinkManager.getSink("default");
        assertEquals(testSink.getMessageList().size(), 1);
        assertEquals(testSink.getMessageList().get(0), "testMessage");
    }

    @Test
    public void testAsyncClient() throws InterruptedException {
        AsyncSuroClient client = clientInjector.getInstance(AsyncSuroClient.class);
        for (int i = 0; i < 1000; ++i) {
            client.send(new Message("routingKey", "testMessage".getBytes()));
        }

        // check the test server whether it got received
        TestMessageRouter.TestSink testSink = (TestMessageRouter.TestSink) sinkManager.getSink("default");

        int count = 0;
        while (client.getSentMessageCount() < 1000 && count < 10) {
            Thread.sleep(1000);
            ++count;
        }
        assertEquals(client.getSentMessageCount(), 1000);
        assertEquals(testSink.getMessageList().size(), 1000);
        for (int i = 0; i < 1000; ++i) {
            assertEquals(testSink.getMessageList().get(0), "testMessage");
        }
    }
}
