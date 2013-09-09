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

import com.google.inject.Injector;
import com.netflix.suro.client.SuroClient;
import com.netflix.suro.message.Message;
import com.netflix.suro.queue.MessageQueue;
import com.netflix.suro.routing.TestMessageRouter;
import com.netflix.suro.server.TestServer;
import com.netflix.suro.sink.SinkManager;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class TestSuroClient {
    private Injector serverInjector;
    private SinkManager sinkManager;

    @Before
    public void createServer() throws Exception {
        // create the test server
        serverInjector = TestServer.start();
        sinkManager = TestMessageRouter.startSinkMakager(serverInjector);
        TestMessageRouter.startMessageRouter(serverInjector);
        serverInjector.getInstance(MessageQueue.class).start();
    }

    @After
    public void shutdown() {
        TestServer.shutdown();
    }

    @Test
    public void testSyncClient() throws TTransportException {
        // create the client
        final Properties clientProperties = new Properties();
        clientProperties.setProperty(ClientConfig.LB_TYPE, "static");
        clientProperties.setProperty(ClientConfig.LB_SERVER, "localhost:7101");
        clientProperties.setProperty(ClientConfig.CLIENT_TYPE, "sync");

        SuroClient client = new SuroClient(clientProperties);

        // send the message
        client.send(new Message("routingKey", "testMessage".getBytes()));

        // check the test server whether it got received
        TestMessageRouter.TestSink testSink = (TestMessageRouter.TestSink) sinkManager.getSink("default");
        assertEquals(testSink.getMessageList().size(), 1);
        assertEquals(testSink.getMessageList().get(0), "testMessage");

        client.shutdown();
    }

    @Test
    public void testAsyncClient() throws InterruptedException {
        // create the client
        final Properties clientProperties = new Properties();
        clientProperties.setProperty(ClientConfig.LB_TYPE, "static");
        clientProperties.setProperty(ClientConfig.LB_SERVER, "localhost:7101");
        clientProperties.setProperty(ClientConfig.ASYNC_TIMEOUT, "0");

        SuroClient client = new SuroClient(clientProperties);

        final int numMessages = 2;
        final int waitTime = 10;

        for (int i = 0; i < numMessages; ++i) {
            client.send(new Message("routingKey", "testMessage".getBytes()));
        }

        // check the test server whether it got received
        TestMessageRouter.TestSink testSink = (TestMessageRouter.TestSink) sinkManager.getSink("default");

        int count = 0;
        while (client.getSentMessageCount() < numMessages && count < waitTime) {
            Thread.sleep(1000);
            ++count;
        }
        assertEquals(client.getSentMessageCount(), numMessages);
        count = 0;
        while (testSink.getMessageList().size() < numMessages && count < waitTime) {
            Thread.sleep(1000);
            ++count;
        }
        assertEquals(testSink.getMessageList().size(), numMessages);
        for (int i = 0; i < numMessages; ++i) {
            assertEquals(testSink.getMessageList().get(0), "testMessage");
        }
    }
}