package com.netflix.suro.servo;

import com.netflix.servo.monitor.BasicCounter;
import com.netflix.servo.monitor.MonitorConfig;

public class Meter {
    private BasicCounter counter;
    private long startTime;

    public Meter(MonitorConfig config) {
        counter = new BasicCounter(config);
        startTime = System.currentTimeMillis() - 1; // to prevent divided by 0
    }

    public void increment() {
        counter.increment();
    }

    public void increment(long delta) {
        counter.increment(delta);
    }

    public double meanRate() {
        return counter.getValue().longValue() / (double) (System.currentTimeMillis() - startTime);
    }
}
