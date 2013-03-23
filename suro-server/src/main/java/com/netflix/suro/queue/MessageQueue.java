package com.netflix.suro.queue;

import com.netflix.suro.thrift.ServiceStatus;
import com.netflix.suro.thrift.SuroServer;
import com.netflix.suro.thrift.TMessageSet;

import java.util.concurrent.TimeUnit;

public abstract class MessageQueue implements SuroServer.Iface {
    protected boolean isTakingTraffic = true;

    public void stopTakingTraffic(){
        this.isTakingTraffic = false;
    }
    public void startTakingTraffic(){
        this.isTakingTraffic = true;
    }

    @Override
    public ServiceStatus getStatus()  {
        if (isTakingTraffic){
            return ServiceStatus.ALIVE;
        } else {
            return ServiceStatus.WARNING;
        }
    }

    public abstract TMessageSet poll(long timeout, TimeUnit unit);
}
