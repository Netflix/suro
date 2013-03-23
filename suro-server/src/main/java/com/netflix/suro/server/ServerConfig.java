package com.netflix.suro.server;

import com.netflix.governator.annotations.Configuration;

public class ServerConfig {
    public static final String SERVER_PORT = "SuroServer.port";
    @Configuration(SERVER_PORT)
    private int port = 7101;
    public int getPort() {
        return port;
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

    public static final String MEMORY_QUEUE_SIZE = "SuroServer.memoryQueueSize";
    @Configuration(MEMORY_QUEUE_SIZE)
    private int memoryQueueSize = 100;
    public int getMemoryQueueSize() {
        return memoryQueueSize;
    }


}
