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

import com.netflix.suro.SuroServer4Test;
import com.netflix.suro.TestFileQueue;
import com.netflix.suro.connection.TestConnectionPool;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestLog4jAppender {
    private Log4jAppender appender = new Log4jAppender();
    private List<SuroServer4Test> collectors;

    @Test
    public void testMemory() throws Exception {
        TestFileQueue.clean();
        List<SuroServer4Test> collectors = TestConnectionPool.startServers(1, 8500);

        appender.setLoadBalancerType("static");
        appender.setLoadBalancerServer("localhost:8500");
        appender.activateOptions();

        LoggingEvent event = mock(LoggingEvent.class);
        when(event.getMessage()).thenReturn(createEventMap());
        when(event.getLevel()).thenReturn(Level.INFO);

        appender.append(event);

        // Make sure client has enough time to drain the intermediary message queue
        Thread.sleep(5000);

        assertEquals(appender.getSentMessageCount(), 1); // it should be successful

        TestConnectionPool.shutdownServers(collectors);

        appender.close();
    }

    @Test
    public void testFile() throws Exception {
        TestFileQueue.clean();
        List<SuroServer4Test> collectors = TestConnectionPool.startServers(1, 8500);

        appender.setAsyncQueueType("file");
        appender.setAsyncFileQueuePath(System.getProperty("java.io.tmpdir"));
        appender.setLoadBalancerType("static");
        appender.setLoadBalancerServer("localhost:8500");
        appender.activateOptions();

        LoggingEvent event = mock(LoggingEvent.class);
        when(event.getMessage()).thenReturn(createEventMap());
        when(event.getLevel()).thenReturn(Level.INFO);

        appender.append(event);

        // Make sure client has enough time to drain the intermediary message queue
        Thread.sleep(5000);

        assertEquals(appender.getSentMessageCount(), 1); // it should be successful

        TestConnectionPool.shutdownServers(collectors);

        appender.close();
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
