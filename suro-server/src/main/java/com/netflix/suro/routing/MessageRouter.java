package com.netflix.suro.routing;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageSetReader;
import com.netflix.suro.message.serde.SerDe;
import com.netflix.suro.message.serde.SerDeFactory;
import com.netflix.suro.queue.MessageQueue;
import com.netflix.suro.server.ServerConfig;
import com.netflix.suro.sink.SinkManager;
import com.netflix.suro.thrift.TMessageSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Singleton
public class MessageRouter {
    static Logger log = LoggerFactory.getLogger(MessageRouter.class);

    private final MessageQueue messageQueue;
    private final RoutingMap routingMap;
    private final SinkManager sinkManager;
    private final SerDeFactory serDeFactory;
    private final ServerConfig config;
    private final ExecutorService executors;

    private final Map<Byte, SerDe> serDeMap = Maps.newHashMap();

    private boolean isRunning;

    @Inject
    public MessageRouter(
            MessageQueue messageQueue,
            RoutingMap routingMap,
            SinkManager sinkManager,
            SerDeFactory serDeFactory,
            ServerConfig config) {
        this.messageQueue = messageQueue;
        this.routingMap = routingMap;
        this.sinkManager = sinkManager;
        this.serDeFactory = serDeFactory;
        this.config = config;

        executors = Executors.newFixedThreadPool(config.getMessageRouterThreads());

        Monitors.registerObject(this);
    }

    public void start() {
        isRunning = true;

        for (int i = 0; i < config.getMessageRouterThreads(); ++i) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    TMessageSet tMessageSet;

                    long waitTime = config.messageRouterDefaultPollTimeout;

                    while (isRunning) {
                        tMessageSet = messageQueue.poll(waitTime, TimeUnit.MILLISECONDS);
                        if (tMessageSet == null) {
                            if (waitTime < config.messageRouterMaxPollTimeout) {
                                waitTime += config.messageRouterDefaultPollTimeout;
                            }
                            continue;
                        }

                        waitTime = config.messageRouterDefaultPollTimeout;
                        processMessageSet(tMessageSet);
                    }
                    // drain remains when shutdown
                    while ((tMessageSet = messageQueue.poll(0, TimeUnit.MILLISECONDS)) != null) {
                        processMessageSet(tMessageSet);
                    }
                }
            });
        }
    }

    public void shutdown() {
        log.info("MessageRouter is shutting down");
        isRunning = false;
        try {
            executors.shutdown();
            executors.awaitTermination(5, TimeUnit.SECONDS);
            if (executors.isTerminated() == false) {
                log.error("MessageDispatcher was not shutdown gracefully within 5 seconds");
            }
            executors.shutdownNow();
        } catch (InterruptedException e) {
            // ignore exceptions while shutting down
        }
    }

    private SerDe getSerDe(byte id) {
        SerDe serde = serDeMap.get(id);
        if (serde == null) {
            serde = serDeFactory.create(id);
            serDeMap.put(id, serde);
        }

        return serde;
    }

    private void processMessageSet(TMessageSet tMessageSet) {
        MessageSetReader reader = new MessageSetReader(tMessageSet);

        for (Message message : reader) {
            RoutingMap.RoutingInfo info = routingMap.getRoutingInfo(message.getRoutingKey());
            SerDe serde = getSerDe(reader.getSerDeId());

            if (info == null) {
                sinkManager.getSink("default").writeTo(message, serde);
            } else if (info != null && info.doFilter(message, serde)) {
                List<String> sinkList = info.getWhere();
                for (String sink : sinkList) {
                    sinkManager.getSink(sink).writeTo(message, serde);
                }
            }
        }
    }
}
