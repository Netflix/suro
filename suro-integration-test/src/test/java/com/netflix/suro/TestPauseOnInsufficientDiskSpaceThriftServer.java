package com.netflix.suro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.netflix.suro.client.SuroClient;
import com.netflix.suro.input.thrift.MessageSetProcessor;
import com.netflix.suro.input.thrift.ServerConfig;
import com.netflix.suro.input.thrift.ThriftServer;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.Message;
import com.netflix.suro.queue.MemoryQueue4Sink;
import com.netflix.suro.queue.Queue4Server;
import com.netflix.suro.routing.MessageRouter;
import com.netflix.suro.routing.RoutingMap;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.sink.SinkManager;
import com.netflix.suro.sink.localfile.LocalFileSink;
import com.netflix.suro.thrift.ServiceStatus;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestPauseOnInsufficientDiskSpaceThriftServer {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private static final String TOPIC_NAME = "tpolq_thrift";

    private volatile boolean hasEnoughSpace = true;

    private LocalFileSink.SpaceChecker createMockedSpaceChecker() {
        LocalFileSink.SpaceChecker spaceChecker = mock(LocalFileSink.SpaceChecker.class);

        doReturn(hasEnoughSpace).when(spaceChecker).hasEnoughSpace();

        return spaceChecker;
    }
    @Test
    public void test() throws Exception {
        ServerConfig config = new ServerConfig();

        final ObjectMapper jsonMapper = new DefaultObjectMapper();

        LocalFileSink.SpaceChecker spaceChecker = mock(LocalFileSink.SpaceChecker.class);

        LocalFileSink sink = new LocalFileSink(
                tempDir.newFolder().getAbsolutePath(),
                null,
                null,
                0,
                "PT1s",
                10,
                new MemoryQueue4Sink(10000),
                100,
                1000,
                true,
                spaceChecker
        );
        sink.open();

        SinkManager sinks = new SinkManager();
        sinks.initialSet(new ImmutableMap.Builder<String, Sink>()
                .put("local", sink).build());

        RoutingMap map = new RoutingMap();
        map.set(new ImmutableMap.Builder<String, RoutingMap.RoutingInfo>()
                .put(TOPIC_NAME, new RoutingMap.RoutingInfo(Lists.newArrayList(new RoutingMap.Route("local", null, null)), null))
                .build());

        MessageSetProcessor msgProcessor = new MessageSetProcessor(
                new Queue4Server(config),
                new MessageRouter(map, sinks, jsonMapper),
                config,
                jsonMapper
        );

        ThriftServer server = new ThriftServer(config, msgProcessor);
        server.start();

        SuroClient client = createSuroClient(server.getPort());

        Thread t = createClientThread(jsonMapper, client, TOPIC_NAME);

        // checking the traffic goes
        for (int i = 0; i < 10; ++i) {
            if (client.getSentMessageCount() > 0) {
                break;
            }
            Thread.sleep(1000);
        }
        assertTrue(client.getSentMessageCount() > 0);

        // shutting down the traffic
        hasEnoughSpace = false;
        int count = 0;
        while (count < 3) {
            if (msgProcessor.getStatus() == ServiceStatus.WARNING) {
                ++count;
            }
        }
        assertEquals(count, 3);

        client.shutdown();
        run.set(false);
        t.join();

        // resuming the traffic
        hasEnoughSpace = true;
        run.set(true);
        SuroClient newClient = createSuroClient(server.getPort());
        t = createClientThread(jsonMapper, newClient, TOPIC_NAME);

        for (int i = 0; i < 10; ++i) {
            if (client.getSentMessageCount() > 0) {
                break;
            }
            Thread.sleep(1000);
        }
        assertTrue(client.getSentMessageCount() > 0);

        run.set(false);
        t.join();
        client.shutdown();

        server.shutdown();

        sink.close();

        assertEquals(sink.getNumOfPendingMessages(), 0);
    }

    private SuroClient createSuroClient(int port) throws java.io.IOException {
        final Properties clientProperties = new Properties();
        clientProperties.setProperty(ClientConfig.CLIENT_TYPE, "sync");
        clientProperties.setProperty(ClientConfig.LB_TYPE, "static");
        clientProperties.setProperty(ClientConfig.LB_SERVER, "localhost:" + port);
        clientProperties.setProperty(ClientConfig.ASYNC_TIMEOUT, "0");
        clientProperties.setProperty(ClientConfig.ENABLE_OUTPOOL, "true");

        return new SuroClient(clientProperties);
    }

    private final AtomicBoolean run = new AtomicBoolean(true);

    private Thread createClientThread(final ObjectMapper jsonMapper, final SuroClient client, final String topicName) {
        Thread t = new Thread(new Runnable() {
            private final RateLimiter rateLimiter = RateLimiter.create(100); // 100 per seconds

            @Override
            public void run() {
                while (run.get()) {
                    rateLimiter.acquire();
                    try {
                        client.send(
                                new Message(
                                        topicName,
                                        jsonMapper.writeValueAsBytes(
                                                new ImmutableMap.Builder<String, Object>()
                                                        .put("f1", "v1")
                                                        .put("f2", "v2")
                                                        .build())));
                    } catch (JsonProcessingException e) {
                        fail();
                    }
                }

                client.shutdown();
            }
        });
        t.start();

        return t;
    }
}
