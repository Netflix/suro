package com.netflix.suro.queue;

import com.google.inject.Singleton;
import org.apache.log4j.Logger;

@Singleton
public class QueueManager {
    static Logger log = Logger.getLogger(QueueManager.class);

    public static final int IN_ERROR = 503;
    public static final int OK = 200;

    private long stopTimeOut = 0L;
    private volatile int status = OK;

    private MessageQueue service;
    public void registerService(MessageQueue service){
        this.service = service;
    }

    public void stopTakingTraffic(){
        if (this.service != null){
            this.service.stopTakingTraffic();
            status = IN_ERROR;
            stopTimeOut = System.currentTimeMillis() + (1*60*1000);
        }
    }

    public void startTakingTraffic(){
        if (System.currentTimeMillis() > stopTimeOut ){
            if (this.service != null){
                this.service.startTakingTraffic();
                status = OK;
            }
        }
    }

    public String getExtendedStatus(){
        return "SuroServer: status=" + status + " - StopTimeOut will expire on :" + new java.util.Date(stopTimeOut)
                + " -- CurrentDate: " + new java.util.Date();
    }

    public int getStatus(){
        return status;
    }
}
