/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.suro.input;

import com.netflix.suro.SuroServer4Test;
import com.netflix.suro.TagKey;
import com.netflix.suro.connection.TestConnectionPool;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestLog4JAppenderWithLog4JConfig {
    private final static Logger LOG = Logger.getLogger(TestLog4JAppenderWithLog4JConfig.class);
    public static final int DEFAULT_WAIT_INTERVAL = 20;

    private List<SuroServer4Test> servers;

    @Before
    public void setup() throws Exception {
        servers = TestConnectionPool.startServers(1);

        Properties props = new Properties();
        props.setProperty("log4j.logger.com.netflix.suro.input","WARN,SURO");
        props.setProperty("log4j.appender.stdout.layout.ConversionPattern", "%5p [%t] (%F:%L) - %m%n");
        props.setProperty("log4j.appender.SURO", "com.netflix.suro.input.Log4jAppender");
        props.setProperty("log4j.appender.SURO.app", "ajjainApp");
        props.setProperty("log4j.appender.SURO.routingKey", "ajjainroutingkey");
        props.setProperty("log4j.appender.SURO.loadBalancerType", "static");
        props.setProperty("log4j.appender.SURO.loadBalancerServer", TestConnectionPool.createConnectionString(servers));
        props.setProperty("log4j.appender.SURO.compression", "0");
        props.setProperty("log4j.appender.SURO.clientType", "sync");

        PropertyConfigurator.configure(props);
    }

    @After
    public void tearDown() throws Exception {
        TestConnectionPool.shutdownServers(servers);
    }

    @Test
    public void testAllMessagesWillBeDeliveredInSeparatedMessageSet() {
        Map<String, String> message = new HashMap<String, String>();
        message.put(TagKey.ROUTING_KEY, "routing_key_1");
        message.put("data", "test");


        final int messageCount = 20;
        for(int i = 0; i < messageCount; ++i) {
            LOG.warn(message);
        }

        waitAndVerify(5000, new Runnable() {
            @Override
            public void run() {
                assertEquals(messageCount, servers.get(0).getMessageSetCount());
                assertEquals(messageCount, servers.get(0).getMessageCount());
            }

        });
    }

    private void waitAndVerify(long millis, Runnable assertion) {
        waitAndVerify(millis, assertion, DEFAULT_WAIT_INTERVAL);
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
}
