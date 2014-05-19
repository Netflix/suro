package com.netflix.suro.queue;

public interface TrafficController {
    void stopTakingTraffic();

    void startTakingTraffic();

    int getStatus();
}
