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

package com.netflix.suro.input.thrift;

import com.netflix.governator.annotations.Configuration;

/**
 * Configuration values for Suro server. Actual configuration values are assigned by
 * wired {@link com.netflix.governator.configuration.ConfigurationProvider}
 */
public class ServerConfig {
    public static final String SERVER_PORT = "SuroServer.port";
    @Configuration(SERVER_PORT)
    private int port = 7101;
    public int getPort() {
        return port;
    }

    public static final String SERVER_STARTUP_TIMEOUT = "SuroServer.startupTimeout";
    @Configuration(SERVER_STARTUP_TIMEOUT)
    private int startupTimeout = 5000;
    public int getStartupTimeout() {
        return startupTimeout;
    }

    public static final String THRIFT_WORKER_THREAD_NUM = "SuroServer.thriftWorkerThreadNum";
    @Configuration(THRIFT_WORKER_THREAD_NUM)
    private int thriftWorkerThreadNum = -1;
    public int getThriftWorkerThreadNum() {
        if(thriftWorkerThreadNum < 0) {
            return Runtime.getRuntime().availableProcessors();
        }

        return thriftWorkerThreadNum;
    }

    public static final String THRIFT_MAX_READ_BUFFER_BYTES = "SuroServer.thriftMaxReadBufferBytes";
    @Configuration(THRIFT_MAX_READ_BUFFER_BYTES)
    private int thriftMaxReadBufferBytes = 1024 * 1024 * 400;
    public int getThriftMaxReadBufferBytes() {
        return thriftMaxReadBufferBytes;
    }

    public static final String SOCKET_SEND_BUFFER_BYTES = "SuroServer.socketSendBufferBytes";
    @Configuration(SOCKET_SEND_BUFFER_BYTES)
    private int socketSendBufferBytes = 1024 * 1024 * 2;
    public int getSocketSendBufferBytes() {
        return socketSendBufferBytes;
    }

    public static final String SOCKET_RECV_BUFFER_BYTES = "SuroServer.socketRecvBufferBytes";
    @Configuration(SOCKET_RECV_BUFFER_BYTES)
    private int socketRecvBufferBytes = 1024 * 1024 * 2;
    public int getSocketRecvBufferBytes() {
        return socketRecvBufferBytes;
    }

    public static final String QUEUE_TYPE = "SuroServer.queueType";
    @Configuration(QUEUE_TYPE)
    private String queueType = "memory";
    public String getQueueType() {
        return queueType;
    }

    public static final String MEMORY_QUEUE_CAPACITY = "SuroServer.memoryQueueCapacity";
    @Configuration(MEMORY_QUEUE_CAPACITY)
    private int memoryQueueCapacity = 100;
    public int getQueueSize() {
        return memoryQueueCapacity;
    }

    public static final String MESSAGE_ROUTER_THREADS = "SuroServer.messageRouterThreads";
    @Configuration(MESSAGE_ROUTER_THREADS)
    private int messageRouterThreads = 2;
    public int getMessageRouterThreads() {
        return messageRouterThreads;
    }

    @Configuration("SuroServer.statusServerPort")
    private int statusServerPort = 7103;
    public int getStatusServerPort() {
        return statusServerPort;
    }

    public static final String FILEQUEUE_PATH = "SuroServer.fileQueuePath";
    @Configuration(FILEQUEUE_PATH)
    private String fileQueuePath = "/logs/suroserver";
    public String getFileQueuePath() {
        return fileQueuePath;
    }

    public static final String FILEQUEUE_NAME = "SuroServer.fileQueueName";
    @Configuration(FILEQUEUE_NAME)
    private String fileQueueName = "messageset";
    public String getFileQueueName() {
        return fileQueueName;
    }

    public static final String FILEQUEUE_GC_PERIOD = "SuroServer.fileQueueGCPeriod";
    @Configuration(FILEQUEUE_GC_PERIOD)
    private String fileQueueGCPeriod = "PT1m";
    public String getFileQueueGCPeriod() {
        return fileQueueGCPeriod;
    }

    public static final String FILEQUEUE_SIZELIMIT = "SuroServer.fileQueueSizeLimit";
    @Configuration(FILEQUEUE_SIZELIMIT)
    private long fileQueueSizeLimit = 10L * 1024L * 1024L * 1024L; // 10 GB
    public long getFileQueueSizeLimit() { return fileQueueSizeLimit; }

    public int messageRouterDefaultPollTimeout = 500;
    public int messageRouterMaxPollTimeout = 2500;
    @Override
    public String toString() {
        return "ServerConfig [port=" + port + ", startupTimeout="
                + startupTimeout + ", thriftWorkerThreadNum="
                + thriftWorkerThreadNum + ", thriftMaxReadBufferBytes="
                + thriftMaxReadBufferBytes + ", socketSendBufferBytes="
                + socketSendBufferBytes + ", socketRecvBufferBytes="
                + socketRecvBufferBytes + ", queueType=" + queueType
                + ", memoryQueueCapacity=" + memoryQueueCapacity
                + ", messageRouterThreads=" + messageRouterThreads
                + ", statusServerPort=" + statusServerPort
                + ", messageRouterDefaultPollTimeout="
                + messageRouterDefaultPollTimeout
                + ", messageRouterMaxPollTimeout="
                + messageRouterMaxPollTimeout + "]";
    }
}
