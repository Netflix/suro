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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.config.DynamicLongProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

import javax.annotation.PreDestroy;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * SinkManager contains map of sink name and its sink object.
 *
 * @author jbae
 */
@Singleton
public class SinkManager {
    private static final Logger log = LoggerFactory.getLogger(SinkManager.class);

    private volatile ImmutableMap<String, Sink> sinkMap = ImmutableMap.of();

    private final DynamicLongProperty sinkOpenCheckInterval;
    private final Subscription sinkCheckSubscription;

    @Inject
    public SinkManager() {
        sinkOpenCheckInterval = new DynamicLongProperty("suro.SinkManager.sinkCheckInterval", 60L);
        sinkCheckSubscription = Observable.interval(sinkOpenCheckInterval.get(), TimeUnit.SECONDS, Schedulers.io())
                .subscribe(new Action1<Long>() {
                    @Override
                    public void call(Long aLong) {
                        log.debug("checking whether all sinks are opened");
                        ImmutableMap<String, Sink> current = sinkMap;
                        for(ImmutableMap.Entry<String, Sink> entry : current.entrySet()) {
                            if(!entry.getValue().isOpened()) {
                                log.warn("sink {} failed to open earlier. trying to open again", entry.getKey());
                                openSink(entry.getKey(), entry.getValue());
                            }
                        }
                    }
                });
    }

    private void openSink(final String name, Sink sink) {
        try {
            sink.open();
        } catch(Throwable t) {
            log.error("failed to open sink: " + name, t);
        }
    }

    private void closeSink(final String name, Sink sink) {
        try {
            sink.close();
        } catch(Throwable t) {
            log.error("failed to close sink: " + name, t);
        }
    }

    public void initialStart() {
        for (Map.Entry<String, Sink> entry : sinkMap.entrySet()) {
            openSink(entry.getKey(), entry.getValue());
        }
    }

    public void initialSet(Map<String, Sink> newSinkMap) {
        if (!sinkMap.isEmpty()) {
            throw new RuntimeException("sinkMap is not empty");
        }
        if(newSinkMap.isEmpty()) {
            log.warn("newSinkMap is empty");
        }
        sinkMap = ImmutableMap.copyOf(newSinkMap);
        log.debug("set initial sinks: {}", newSinkMap.keySet());
    }

    /**
     * actual work is done asynchronously in background thread
     */
    public void set(final Map<String, Sink> newSinkMap) {
        if(newSinkMap.isEmpty()) {
            log.warn("newSinkMap is empty");
        }
        Observable.just(newSinkMap)
            .observeOn(Schedulers.io())
            .subscribe(new Action1<Map<String, Sink>>() {
                @Override
                public void call(Map<String, Sink> stringSinkMap) {
                    log.debug("setting sinks: {}", newSinkMap.keySet());
                    ImmutableMap<String, Sink> newMap = ImmutableMap.copyOf(newSinkMap);
                    // open sinks for newMap
                    for (Map.Entry<String, Sink> entry : newMap.entrySet()) {
                        openSink(entry.getKey(), entry.getValue());
                    }
                    log.warn("opened new sinks: {}", newMap.keySet());
                    // swap the reference
                    ImmutableMap<String, Sink> oldMap = sinkMap;
                    sinkMap = newMap;
                    log.warn("applied new sinks: {}", newSinkMap.keySet());
                    // close sinks from oldMap
                    for (Map.Entry<String, Sink> entry : oldMap.entrySet()) {
                        closeSink(entry.getKey(), entry.getValue());
                    }
                    log.warn("closed old sinks: {}", oldMap.keySet());
                }
            });
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
        sinkCheckSubscription.unsubscribe();
        for (Map.Entry<String, Sink> entry : sinkMap.entrySet()) {
            closeSink(entry.getKey(), entry.getValue());
        }
    }
}
