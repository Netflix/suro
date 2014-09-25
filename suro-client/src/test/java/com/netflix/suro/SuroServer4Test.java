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

import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageSetReader;
import com.netflix.suro.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;

import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class SuroServer4Test implements SuroServer.Iface {
    private long uptime = System.currentTimeMillis();
    private int port = 0;
    private THsHaServer server = null;
    private TNonblockingServerSocket transport = null;
    private SuroServer.Processor processor = null;
    private Map<String, AtomicLong> counters = new HashMap<String, AtomicLong>();
    private List<TMessageSet> messageSetList = new LinkedList<TMessageSet>();

    public SuroServer4Test() {
        counters.put("messageSetCount",new AtomicLong(0l));
        counters.put("messageCount",new AtomicLong(0l));
    }

    private boolean tryLater = false;

    public void setTryLater() {
        tryLater = true;
    }

    public void cancelTryLater() {
        tryLater = false;
    }

    private boolean holdConnection = false;

    public void setHoldConnection() {
        holdConnection = true;
    }

    public void cancelHoldConnection() {
        holdConnection = false;
        latch.countDown();
    }

    public int getPort() {
        return port;
    }

    private CountDownLatch latch = new CountDownLatch(1);
    public Result process(TMessageSet messageSet) throws TException {
        Result result = new Result();
        if (tryLater) {
            result.setResultCode(ResultCode.QUEUE_FULL);
        } else {
            if (!holdConnection) {
                handleMessages(messageSet, result);
            } else {
                try {
                    latch.await();
                    handleMessages(messageSet, result);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }

    private void handleMessages(TMessageSet messageSet, Result result) {
        System.out.println(this + "=====================>>>>>>>>>>>>>>>>>>>>     getting a new messageSet" + counters.get("messageSetCount").getAndIncrement());
//        for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
//            System.out.println(e);
//        }
        messageSetList.add(messageSet);
        int count = 0;
        for (Message m : new MessageSetReader(messageSet)) {
            counters.get("messageCount").incrementAndGet();
            ++count;
        }
        System.out.println(this + "=====================>>>>>>>>>>>>>>>>>>>>     getting a new messageSet: " + count);
        result.setMessage("my message");
        result.setResultCode(ResultCode.OK);
    }

    public TMessageSet getMessageSet(int index) {
        return messageSetList.get(index);
    }

    public long getCounter(String key) throws TException {
        return counters.get(key).get();
    }

    public String getName() throws TException {
        return "Test Server";
    }

    public ServiceStatus getStatus() throws TException {
        return ServiceStatus.ALIVE;
    }

    public long getUptime() throws TException {
        return uptime;
    }

    public String getVersion() throws TException {
        return "Test Server - V0";
    }

    public  void start() throws Exception {
        transport = new TNonblockingServerSocket(port);
        processor =  new SuroServer.Processor(this);

        THsHaServer.Args serverArgs = new THsHaServer.Args(transport);
        serverArgs.processor(processor);
        serverArgs.workerThreads(2);

        server = new THsHaServer(serverArgs);
        System.out.println("Server started on port:" + port);

        Thread t = new Thread() {

            @Override
            public void run() {
                server.serve();
            }

        };
        t.start();

        Field serverSocketField = TNonblockingServerSocket.class.getDeclaredField("serverSocket_");
        serverSocketField.setAccessible(true);
        ServerSocket serverSocket = (ServerSocket) serverSocketField.get(transport);
        port = serverSocket.getLocalPort();
    }

    public  long getMessageSetCount() {
        return counters.get("messageSetCount").get();
    }

    public long getMessageCount() {
        return counters.get("messageCount").get();
    }

    public long shutdown() {
        server.stop();
        transport.close();
        System.out.println("shutdown STC");
        try {Thread.sleep(1000);} catch (Exception e) { e.printStackTrace(); }
        return 0;
    }
}
