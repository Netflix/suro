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

package com.netflix.suro;

import com.netflix.governator.annotations.Configuration;
import com.netflix.suro.input.JsonLog4jFormatter;
import com.netflix.suro.message.serde.JsonSerDe;
import com.netflix.suro.message.SerDe;
import com.netflix.suro.message.serde.SerDeFactory;

public class ClientConfig {
    public static final String CONNECTION_TIMEOUT = "SuroClient.connectionTimeout";
    @Configuration(CONNECTION_TIMEOUT)
    private int connectionTimeout = 5000; // millisecond
    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public static final String ENABLE_OUTPOOL = "SuroClient.enableOutPool";
    @Configuration(ENABLE_OUTPOOL)
    private boolean enableOutPool = false;
    public boolean getEnableOutPool() { return enableOutPool; }

    public static final String CONNECTION_SWEEP_INTERVAL = "SuroClient.connectionSweepInterval";
    @Configuration(CONNECTION_SWEEP_INTERVAL)
    private int connectionSweepInterval = 3600; // second
    public int getConnectionSweepInterval() {
        return connectionSweepInterval;
    }

    public static final String LOG4J_FORMATTER = "SuroClient.log4jFormatter";
    @Configuration(LOG4J_FORMATTER)
    private String log4jFormatter = JsonLog4jFormatter.class.toString();
    public String getLog4jFormatter() {
        return log4jFormatter;
    }

    public static final String LOG4J_DATETIMEFORMAT = "SuroClient.log4jDateTimeFormat";
    @Configuration(LOG4J_DATETIMEFORMAT)
    private String log4jDateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss,SSS";
    public String getLog4jDateTimeFormat() {
        return log4jDateTimeFormat;
    }

    public static final String LOG4J_ROUTING_KEY = "SuroClient.log4jRoutingKey";
    @Configuration(LOG4J_ROUTING_KEY)
    private String log4jRoutingKey = "";
    public String getLog4jRoutingKey() {
        return log4jRoutingKey;
    }

    public static final String APP = "SuroClient.app";
    @Configuration(APP)
    private String app = "defaultApp";
    public String getApp() {
        return app;
    }

    public static final String RETRY_COUNT = "SuroClient.retryCount";
    @Configuration(RETRY_COUNT)
    private int retryCount = 5;
    public int getRetryCount() {
        return retryCount;
    }

    public static final String SERDE = "SuroClient.serDe";
    @Configuration(SERDE)
    private String serde = JsonSerDe.class.getCanonicalName();
    public SerDe getSerDe() {
        return SerDeFactory.create(serde);
    }

    public static final String COMPRESSION = "SuroClient.compression";
    @Configuration(COMPRESSION)
    private int compression = 1;
    public int getCompression() {
        return compression;
    }

    public static final String CLIENT_TYPE = "SuroClient.clientType";
    @Configuration(CLIENT_TYPE)
    private String clientType = "async";
    public String getClientType() {
        return clientType;
    }

    public static final String ASYNC_SENDER_THREADS = "SuroClient.asyncSenderThreads";
    @Configuration(ASYNC_SENDER_THREADS)
    private int senderThreads = 3;
    public int getAsyncSenderThreads() {
        return senderThreads;
    }

    public static final String ASYNC_BATCH_SIZE = "SuroClient.asyncBatchSize";
    @Configuration(ASYNC_BATCH_SIZE)
    private int asyncBatchSize = 200;
    public int getAsyncBatchSize() {
        return asyncBatchSize;
    }

    public static final String ASYNC_TIMEOUT = "SuroClient.asyncTimeout";
    @Configuration(ASYNC_TIMEOUT)
    private int asyncTimeout = 5000;
    public int getAsyncTimeout() {
        return asyncTimeout;
    }

    public static final String ASYNC_QUEUE_TYPE = "SuroClient.asyncQueueType";
    @Configuration(ASYNC_QUEUE_TYPE)
    private String asyncQueueType = "memory";
    public String getAsyncQueueType() {
        return asyncQueueType;
    }

    public static final String ASYNC_MEMORYQUEUE_CAPACITY = "SuroClient.asyncMemoryQueueCapacity";
    @Configuration(ASYNC_MEMORYQUEUE_CAPACITY)
    private int asyncMemoryQueueCapacity = 10000;
    public int getAsyncMemoryQueueCapacity() {
        return asyncMemoryQueueCapacity;
    }

    public static final String ASYNC_JOBQUEUE_CAPACITY = "SuroClient.asyncJobQueueCapacity";
    @Configuration(ASYNC_JOBQUEUE_CAPACITY)
    private int asyncJobQueueCapacity = 0;
    public int getAsyncJobQueueCapacity() {
        if (asyncJobQueueCapacity == 0) {
            return asyncMemoryQueueCapacity / asyncBatchSize;
        } else {
            return asyncJobQueueCapacity;
        }
    }

    public static final String ASYNC_FILEQUEUE_PATH = "SuroClient.asyncFileQueuePath";
    @Configuration(ASYNC_FILEQUEUE_PATH)
    private String asyncFileQueuePath = "/logs/suroclient";
    public String getAsyncFileQueuePath() {
        return asyncFileQueuePath;
    }

    public static final String ASYNC_FILEQUEUE_NAME = "SuroClient.asyncFileQueueName";
    @Configuration(ASYNC_FILEQUEUE_NAME)
    private String asyncFileQueueName = "default";
    public String getAsyncFileQueueName() {
        return asyncFileQueueName;
    }

    public static final String FILEQUEUE_GC_PERIOD = "SuroClient.asyncFileQueueGCPeriod";
    @Configuration(FILEQUEUE_GC_PERIOD)
    private String fileQueueGCPeriod = "PT1m";
    public String getAsyncFileQueueGCPeriod() {
        return fileQueueGCPeriod;
    }

    public static final String FILEQUEUE_SIZELIMIT = "SuroClient.fileQueueSizeLimit";
    @Configuration(FILEQUEUE_SIZELIMIT)
    private long fileQueueSizeLimit = 10L * 1024L * 1024L * 1024L; // 10 GB
    public long getFileQueueSizeLimit() { return fileQueueSizeLimit; }

    public static final String LB_TYPE = "SuroClient.loadBalancerType";
    @Configuration(LB_TYPE)
    private String loadBalancerType;
    public String getLoadBalancerType() {
        return loadBalancerType;
    }

    public static final String LB_SERVER = "SuroClient.loadBalancerServer";
    @Configuration(LB_SERVER)
    private String loadBalancerServer;
    public String getLoadBalancerServer() {
        return loadBalancerServer;
    }

    public static final String MINIMUM_RECONNECT_TIME_INTERVAL = "SuroClient.minimum.reconnect.timeInterval";
    @Configuration(MINIMUM_RECONNECT_TIME_INTERVAL)
    private int minimumReconnectTimeInterval = 90000;
    public int getMinimumReconnectTimeInterval() { return minimumReconnectTimeInterval; }

    public static final String RECONNECT_INTERVAL = "SuroClient.reconnect.interval";
    @Configuration(RECONNECT_INTERVAL)
    private int reconnectInterval = 240;
    public int getReconnectInterval() { return reconnectInterval; }

    public static final String RECONNECT_TIME_INTERVAL = "SuroClient.reconnect.timeInterval";
    @Configuration(RECONNECT_TIME_INTERVAL)
    private int reconnectTimeInterval = 300000;
    public int getReconnectTimeInterval() { return reconnectTimeInterval; }
}
