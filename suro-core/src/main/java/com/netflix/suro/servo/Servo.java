package com.netflix.suro.servo;

import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.*;
import com.netflix.servo.tag.BasicTagList;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class Servo {
    private static final ConcurrentMap<MonitorConfig, Counter> counters = new ConcurrentHashMap<>();
    private static final ConcurrentMap<MonitorConfig, Timer> timers = new ConcurrentHashMap<>();
    private static final ConcurrentMap<MonitorConfig, NumberGauge> gauges = new ConcurrentHashMap<>();

    private Servo() {
    }

    public static Counter getCounter(MonitorConfig config) {
        Counter v = counters.get(config);
        if (v != null) return v;
        else {
            Counter counter = new BasicCounter(config);
            Counter prevCounter = counters.putIfAbsent(config, counter);
            if (prevCounter != null) return prevCounter;
            else {
                DefaultMonitorRegistry.getInstance().register(counter);
                return counter;
            }
        }
    }

    public static Counter getCounter(String name, String... tags) {
        MonitorConfig.Builder cfgBuilder = MonitorConfig.builder(name);
        if (tags.length > 0) {
            cfgBuilder.withTags(BasicTagList.of(tags));
        }
        return getCounter(cfgBuilder.build());
    }

    public static Timer getTimer(MonitorConfig config) {
        Timer v = timers.get(config);
        if (v != null) return v;
        else {
            Timer timer = new BasicTimer(config, TimeUnit.SECONDS);
            Timer prevTimer = timers.putIfAbsent(config, timer);
            if (prevTimer != null) return prevTimer;
            else {
                DefaultMonitorRegistry.getInstance().register(timer);
                return timer;
            }
        }
    }

    public static Timer getTimer(String name, String... tags) {
        MonitorConfig.Builder cfgBuilder = MonitorConfig.builder(name);
        if (tags.length > 0) {
            cfgBuilder.withTags(BasicTagList.of(tags));
        }
        return getTimer(cfgBuilder.build());
    }

    @SuppressWarnings("unchecked")
    public static <T extends Number> T getNumberGauge(MonitorConfig config, T number) {
        NumberGauge v = gauges.get(config);
        if (v != null) return (T) v.getValue(0);
        else {
            NumberGauge gauge = new NumberGauge(config, number);
            NumberGauge prev = gauges.putIfAbsent(config, gauge);
            if (prev != null) return (T) prev.getValue(0);
            else {
                DefaultMonitorRegistry.getInstance().register(gauge);
                return (T) gauge.getValue(0);
            }
        }
    }

    public static <T extends Number> T getNumberGauge(String name, T number) {
        return getNumberGauge(MonitorConfig.builder(name).build(), number);
    }
}