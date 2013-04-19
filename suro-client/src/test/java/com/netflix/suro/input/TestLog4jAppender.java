package com.netflix.suro.input;

import com.netflix.suro.SuroServer4Test;
import com.netflix.suro.client.async.TestFileQueue;
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
    public void test() throws Exception {
        TestFileQueue.clean();
        List<SuroServer4Test> collectors = TestConnectionPool.startServers(1, 8500);

        appender.setAsyncQueueType("file");
        appender.setFileQueuePath(System.getProperty("java.io.tmpdir"));
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

        return map;
    }
}
