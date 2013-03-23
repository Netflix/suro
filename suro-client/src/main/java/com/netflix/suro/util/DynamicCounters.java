package com.netflix.suro.util;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.MonitorConfig;

public class DynamicCounters {
    private Table<String, String, MonitorConfig> configTable = HashBasedTable.create();

    public void increment(String name, String app) {
        DynamicCounter.increment(getMonitor(name, app));
    }

    public MonitorConfig getMonitor(String name, String app) {
        MonitorConfig config = configTable.get(name, app);
        if (config == null) {
            config = MonitorConfig.builder(name + "_" + app).build();
            configTable.put(name, app, config);
        }
        return config;
    }

    public void increment(String name, String app, long delta) {
        DynamicCounter.increment(getMonitor(name, app), delta);
    }
}
