package com.netflix.suro.server;

import com.google.inject.Injector;
import com.netflix.governator.lifecycle.LifecycleManager;
import com.netflix.suro.SuroPlugin;
import com.netflix.suro.SuroServer;
import com.netflix.suro.TestUtils;
import com.netflix.suro.input.DynamicPropertyInputConfigurator;
import com.netflix.suro.routing.DynamicPropertyRoutingMapConfigurator;
import com.netflix.suro.routing.TestMessageRouter;
import com.netflix.suro.sink.DynamicPropertySinkConfigurator;
import org.junit.rules.ExternalResource;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class SuroServerExternalResource extends ExternalResource {
    private int statusPort;
    private int serverPort;
    private AtomicReference<Injector> injector = new AtomicReference<Injector>();

    private String sinkDesc = "{\n" +
            "    \"default\": {\n" +
            "        \"type\": \"TestSink\",\n" +
            "        \"message\": \"default\"\n" +
            "    },\n" +
            "    \"sink1\": {\n" +
            "        \"type\": \"TestSink\",\n" +
            "        \"message\": \"sink1\"\n" +
            "    }\n" +
            "}";

    private String mapDesc="{}";
    private String inputConfig = "[\n" +
            "    {\n" +
            "        \"type\": \"thrift\"\n" +
            "    }\n" +
            "]";

    public SuroServerExternalResource() {}
    public SuroServerExternalResource(String inputConfig, String sinkDesc, String mapDesc) {
        this.inputConfig = inputConfig;
        this.sinkDesc = sinkDesc;
        this.mapDesc = mapDesc;
    }

    @Override
    protected void before() throws Exception {
        statusPort = TestUtils.pickPort();
        serverPort = TestUtils.pickPort();

        Properties props = new Properties();
        props.put("SuroServer.statusServerPort", Integer.toString(statusPort));
        props.put("SuroServer.port", Integer.toString(serverPort));
        props.put(DynamicPropertySinkConfigurator.SINK_PROPERTY, sinkDesc);
        props.put(DynamicPropertyInputConfigurator.INPUT_CONFIG_PROPERTY, inputConfig);
        if (mapDesc != null) {
            props.put(DynamicPropertyRoutingMapConfigurator.ROUTING_MAP_PROPERTY, mapDesc);
        }

        SuroServer.create(injector, props, new SuroPlugin() {
            @Override
            protected void configure() {
                this.addSinkType("TestSink", TestMessageRouter.TestMessageRouterSink.class);
            }
        });
        injector.get().getInstance(LifecycleManager.class).start();
        injector.get().getInstance(StatusServer.class).waitUntilStarted();
    }

    @Override
    protected void after() {
        injector.get().getInstance(LifecycleManager.class).close();
    }

    public int getServerPort() {
        return serverPort;
    }
    public int getStatusPort() { return statusPort; }
    public Injector getInjector() { return injector.get(); }
}
