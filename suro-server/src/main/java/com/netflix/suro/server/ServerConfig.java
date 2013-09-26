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

package com.netflix.suro.server;

import com.netflix.governator.annotations.Configuration;

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
    private int thriftWorkerThreadNum = 10;
    public int getThriftWorkerThreadNum() {
        return thriftWorkerThreadNum;
    }

    public static final String THRIFT_MAX_READ_BUFFER_BYTES = "SuroServer.thriftMaxReadBufferBytes";
    @Configuration(THRIFT_MAX_READ_BUFFER_BYTES)
    private int thriftMaxReadBufferBytes = 1024 * 1024 * 400;
    public int getThriftMaxReadBufferBytes() {
        return thriftMaxReadBufferBytes;
    }

    public static final String SOCKET_SEND_BUFFER_BYTES = "SuroServr.socketSendBufferBytes";
    @Configuration(SOCKET_SEND_BUFFER_BYTES)
    private int socketSendBufferBytes = 1024 * 1024 * 2;
    public int getSocketSendBufferBytes() {
        return socketSendBufferBytes;
    }

    public static final String SOCKET_RECV_BUFFER_BYTES = "SuroServr.socketRecvBufferBytes";
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

    public static final String MEMORY_QUEUE_SIZE = "SuroServer.memoryQueueSize";
    @Configuration(MEMORY_QUEUE_SIZE)
    private int memoryQueueSize = 100;
    public int getQueueSize() {
        return memoryQueueSize;
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

    public int messageRouterDefaultPollTimeout = 500;
    public int messageRouterMaxPollTimeout = 10000;
    @Override
    public String toString() {
        return "ServerConfig [port=" + port + ", startupTimeout="
                + startupTimeout + ", thriftWorkerThreadNum="
                + thriftWorkerThreadNum + ", thriftMaxReadBufferBytes="
                + thriftMaxReadBufferBytes + ", socketSendBufferBytes="
                + socketSendBufferBytes + ", socketRecvBufferBytes="
                + socketRecvBufferBytes + ", queueType=" + queueType
                + ", memoryQueueSize=" + memoryQueueSize
                + ", messageRouterThreads=" + messageRouterThreads
                + ", statusServerPort=" + statusServerPort
                + ", messageRouterDefaultPollTimeout="
                + messageRouterDefaultPollTimeout
                + ", messageRouterMaxPollTimeout="
                + messageRouterMaxPollTimeout + "]";
    }
}
