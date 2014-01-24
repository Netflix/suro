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

import com.google.common.annotations.VisibleForTesting;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.client.SuroClient;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.SerDe;
import com.netflix.suro.message.StringSerDe;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

public class Log4jAppender extends AppenderSkeleton {
    protected static String localHostAddr = null;
    static {
        try {
            localHostAddr = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            localHostAddr = "N/A";
        }
    }

    private String formatterClass = JsonLog4jFormatter.class.getName();
    public void setFormatterClass(String formatterClass) {
        this.formatterClass = formatterClass;
    }
    public String getFormatterClass() {
        return formatterClass;
    }

    private String datetimeFormat = "yyyy-MM-dd'T'HH:mm:ss,SSS";
    public void setDatetimeFormat(String datetimeFormat) {
        this.datetimeFormat = datetimeFormat;
    }
    public String getDatetimeFormat() {
        return datetimeFormat;
    }

    private String routingKey = "";
    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }
    public String getRoutingKey() { return routingKey; }

    private String app = "defaultApp";
    public void setApp(String app) {
        this.app = app;
    }
    public String getApp() {
        return app;
    }

    private String compression = "1";
    public void setCompression(String compression) {
        this.compression = compression;
    }

    public String getCompression() {
        return compression;
    }

    private String loadBalancerType = "eureka";
    public void setLoadBalancerType(String loadBalancerType) {
        this.loadBalancerType = loadBalancerType;
    }
    private String getLoadBalancerType() {
        return loadBalancerType;
    }

    private String loadBalancerServer;
    public void setLoadBalancerServer(String loadBalancerServer) {
        this.loadBalancerServer = loadBalancerServer;
    }
    private String getLoadBalancerServer() {
        return loadBalancerServer;
    }

    private String asyncQueueType = "memory";
    public void setAsyncQueueType(String asyncQueueType) {
        this.asyncQueueType = asyncQueueType;
    }
    public String getAsyncQueueType() {
        return asyncQueueType;
    }

    private String asyncMemoryQueueCapacity = "10000";
    public void setAsyncMemoryQueueCapacity(String memoryQueueCapacity) {
        this.asyncMemoryQueueCapacity = memoryQueueCapacity;
    }
    public String getAsyncMemoryQueueCapacity() {
        return asyncMemoryQueueCapacity;
    }

    private String asyncFileQueuePath = "/logs/suroClient";
    public String getAsyncFileQueuePath() {
        return asyncFileQueuePath;
    }
    public void setAsyncFileQueuePath(String fileQueuePath) {
        this.asyncFileQueuePath = fileQueuePath;
    }

    private String clientType = "async";
    public void setClientType(String clientType) {
        this.clientType = clientType;
    }
    public String getClientType() {
        return clientType;
    }

    private Log4jFormatter formatter;
    @VisibleForTesting
    protected SuroClient client;

    @Override
    public void activateOptions() {
        client = new SuroClient(createProperties());

        try {
            formatter = (Log4jFormatter)
                    Class.forName(formatterClass).getDeclaredConstructor(ClientConfig.class)
                            .newInstance(client.getConfig());
        } catch (Exception e) {
            formatter = new JsonLog4jFormatter(client.getConfig());
        }
    }

    private Properties createProperties() {
        Properties properties = new Properties();
        properties.setProperty(ClientConfig.LOG4J_FORMATTER, formatterClass);
        properties.setProperty(ClientConfig.LOG4J_DATETIMEFORMAT, datetimeFormat);
        properties.setProperty(ClientConfig.LOG4J_ROUTING_KEY, routingKey);
        properties.setProperty(ClientConfig.APP, app);
        properties.setProperty(ClientConfig.COMPRESSION, compression);
        properties.setProperty(ClientConfig.LB_TYPE, loadBalancerType);
        properties.setProperty(ClientConfig.LB_SERVER, loadBalancerServer);
        properties.setProperty(ClientConfig.ASYNC_MEMORYQUEUE_CAPACITY, asyncMemoryQueueCapacity);
        properties.setProperty(ClientConfig.ASYNC_QUEUE_TYPE, asyncQueueType);
        properties.setProperty(ClientConfig.ASYNC_FILEQUEUE_PATH, asyncFileQueuePath);
        properties.setProperty(ClientConfig.CLIENT_TYPE, clientType);

        return properties;
    }

    @Override
    public void doAppend(LoggingEvent event) {
        this.append(event);
    }

    private SerDe<String> serDe = new StringSerDe();
    @Override
    protected void append(LoggingEvent event) {
        String result = formatter.format(event);
        client.send(new Message(
                formatter.getRoutingKey(),
                result.getBytes()));
    }

    @Override
    public void close() {
        client.shutdown();
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    public long getSentMessageCount() {
        return client.getSentMessageCount();
    }
}
