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

package com.netflix.suro.input;

import com.netflix.suro.ClientConfig;
import com.netflix.suro.SuroServer4Test;
import com.netflix.suro.client.SuroClient;
import com.netflix.suro.connection.TestConnectionPool;
import com.netflix.suro.message.Message;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class TestLog4jAppender {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    public static final int DEFAULT_WAIT_INTERVAL = 20;
    private Log4jAppender appender = new Log4jAppender();
    private List<SuroServer4Test> servers;

    @Before
    public void setup() throws Exception {
        servers = TestConnectionPool.startServers(1);
    }

    @After
    public void clean() {
        TestConnectionPool.shutdownServers(servers);
    }

    private void sleepThrough(long millis) {
        long remaining = millis;
        while( remaining > 0 ) {
            long start = System.currentTimeMillis();
            try{
                Thread.sleep(remaining);
            } catch (InterruptedException e){ }

            remaining -= (System.currentTimeMillis() - start);
        }
    }

    private void waitAndVerify(long millis, Runnable assertion, long waitInterval) {
        long remaining = millis;
        while(remaining > 0) {
            try{
                assertion.run();

                // Assertion is successful, so we don't need to wait any more
                return;
            } catch(Throwable t) {
                sleepThrough(waitInterval);
                remaining -= waitInterval;
            }
        }

        // Last attempt after timeout, so we will get assertion failure if
        // there is one.
        assertion.run();
    }

    private void waitAndVerify(long millis, Runnable assertion) {
        waitAndVerify(millis, assertion, DEFAULT_WAIT_INTERVAL);
    }

    @Test
    public void testMemory() throws Exception {
        appender.setLoadBalancerType("static");
        appender.setLoadBalancerServer(TestConnectionPool.createConnectionString(servers));
        appender.activateOptions();

        LoggingEvent event = mock(LoggingEvent.class);
        when(event.getMessage()).thenReturn(createEventMap());
        when(event.getLevel()).thenReturn(Level.INFO);

        appender.append(event);

        // Make sure client has enough time to drain the intermediary message queue
        waitAndVerify(5000, new Runnable(){

            @Override
            public void run() {
                assertEquals(appender.getSentMessageCount(), 1); // it should be successful
            }
        });



        appender.close();
    }

    @Test
    public void testFile() throws Exception {
        appender.setAsyncQueueType("file");
        appender.setAsyncFileQueuePath(tempDir.newFolder().getAbsolutePath());
        appender.setLoadBalancerType("static");
        appender.setLoadBalancerServer(TestConnectionPool.createConnectionString(servers));
        appender.activateOptions();

        LoggingEvent event = mock(LoggingEvent.class);
        when(event.getMessage()).thenReturn(createEventMap());
        when(event.getLevel()).thenReturn(Level.INFO);

        appender.append(event);

        // Make sure client has enough time to drain the intermediary message queue
        waitAndVerify(15000, new Runnable() {
            public void run() {
                assertEquals(appender.getSentMessageCount(), 1);
            }
        });


        appender.close();
    }

    @Test
    public void testLog4jFormatter() {
        appender.setFormatterClass("com.netflix.suro.input.StaticLog4jFormatter");

        appender.setLoadBalancerType("static");
        appender.setLoadBalancerServer(TestConnectionPool.createConnectionString(servers));
        appender.setClientType("sync");
        appender.setRoutingKey("testRoutingKey");
        appender.activateOptions();

        appender.client = mock(SuroClient.class);
        doNothing().when(appender.client).send(any(Message.class));

        LoggingEvent event = mock(LoggingEvent.class);
        when(event.getMessage()).thenReturn("string log");
        when(event.getLevel()).thenReturn(Level.INFO);
        appender.append(event);

        ArgumentCaptor<Message> argument = ArgumentCaptor.forClass(Message.class);
        verify(appender.client).send(argument.capture());
        assertEquals(argument.getValue().getRoutingKey(), "testRoutingKey");
        String[] v0 = new String(argument.getValue().getPayload()).split("\t");
        String[] v1 = new StaticLog4jFormatter(new ClientConfig()).format(event).split("\t");

        assertEquals(v0.length, v1.length);

        for (int i = 0; i < v0.length; ++i) {
            if (i == 0) {
                assertEquals(v0[0].split(":")[0], v1[0].split(":")[0]);
            } else {
                assertEquals(v0[i], v1[i]);
            }
        }
    }

    private Map<String, String> createEventMap() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("one", "1");
        map.put("two", "2");
        map.put("three", "3");
        map.put("routingKey", "routingKey");

        return map;
    }
}
