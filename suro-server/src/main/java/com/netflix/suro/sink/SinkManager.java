package com.netflix.suro.sink;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

@Singleton
public class SinkManager {
    static Logger log = Logger.getLogger(SinkManager.class);

    private final ConcurrentMap<String, Sink> sinkMap = Maps.newConcurrentMap();
    private final ObjectMapper jsonMapper;

    @Inject
    public SinkManager(ObjectMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public void build(String desc) {
        try {
            Map<String, Sink> newSinkMap = jsonMapper.readValue(desc, new TypeReference<Map<String, Sink>>(){});
            if (newSinkMap.containsKey("default") == false) {
                throw new IllegalStateException("default sink should be defined");
            }

            for (Map.Entry<String, Sink> sink : sinkMap.entrySet()) {
                if (newSinkMap.containsKey(sink.getKey()) == false) { // removed
                    Sink removedSink = sinkMap.remove(sink.getKey());
                    if (removedSink != null) {
                        removedSink.close();
                    }
                }
            }

            for (Map.Entry<String, Sink> sink : newSinkMap.entrySet()) {
                if (sinkMap.containsKey(sink.getKey()) == false) { // added
                    sink.getValue().open();
                    sinkMap.put(sink.getKey(), sink.getValue());
                }
            }

        } catch (Exception e) {
            log.error("Exception on building SinkManager: " + e.getMessage(), e);
            if (sinkMap.isEmpty()) {
                throw new RuntimeException("At least one sink is needed");
            }
        }
    }

    public Sink getSink(String routingKey) {
        Sink sink = sinkMap.get(routingKey);
        if (sink == null) {
            sink = sinkMap.get("default");
        }
        return sink;
    }

    public String reportSinkStat() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Sink> entry : sinkMap.entrySet()) {
            sb.append(entry.getKey()).append(':').append(entry.getValue().getStat()).append('\n');
        }

        return sb.toString();
    }

    public void shutdown() {
        log.info("SinkManager shuting down");
        for (Map.Entry<String, Sink> entry : sinkMap.entrySet()) {
           entry.getValue().close();
        }
        sinkMap.clear();
    }
}
