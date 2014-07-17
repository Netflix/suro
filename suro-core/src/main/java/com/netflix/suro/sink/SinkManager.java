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

package com.netflix.suro.sink;

import com.google.common.collect.Maps;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * SinkManager contains map of sink name and its sink object.
 *
 * @author jbae
 */
@Singleton
public class SinkManager {
    private static final Logger log = LoggerFactory.getLogger(SinkManager.class);

    private final ConcurrentMap<String, Sink> sinkMap = Maps.newConcurrentMap();

    public void initialStart() {
        for (Sink sink : sinkMap.values()) {
            sink.open();
        }
    }

    public void initialSet(Map<String, Sink> newSinkMap) {
        if (!sinkMap.isEmpty()) {
            throw new RuntimeException("sinkMap is not empty");
        }

        for (Map.Entry<String, Sink> sink : newSinkMap.entrySet()) {
            log.info(String.format("Adding sink '%s'", sink.getKey()));
            sinkMap.put(sink.getKey(), sink.getValue());
        }
    }

    public void set(Map<String, Sink> newSinkMap) {
        try {
            for (Map.Entry<String, Sink> sink : sinkMap.entrySet()) {
                if ( !newSinkMap.containsKey( sink.getKey() ) ) { // removed
                    Sink removedSink = sinkMap.remove(sink.getKey());
                    if (removedSink != null) {
                        log.info(String.format("Removing sink '%s'", sink.getKey()));
                        removedSink.close();
                    }
                }
            }

            for (Map.Entry<String, Sink> sink : newSinkMap.entrySet()) {
                if (!sinkMap.containsKey( sink.getKey() ) ) { // added
                    log.info(String.format("Adding sink '%s'", sink.getKey()));
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

    public Sink getSink(String id) {
        Sink sink = sinkMap.get(id);
        if (sink == null) {
            sink = sinkMap.get("default");
        }
        return sink;
    }

    public String reportSinkStat() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Sink> entry : sinkMap.entrySet()) {
            sb.append(entry.getKey()).append(':').append(entry.getValue().getStat()).append("\n\n");
        }

        return sb.toString();
    }

    public Collection<Sink> getSinks() {
        return sinkMap.values();
    }

    @PreDestroy
    public void shutdown() {
        log.info("SinkManager shutting down");
        for (Map.Entry<String, Sink> entry : sinkMap.entrySet()) {
           entry.getValue().close();
        }
        sinkMap.clear();
    }
}
