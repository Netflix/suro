package com.netflix.suro.client;

import com.netflix.suro.ClientConfig;
import com.netflix.suro.message.Message;

import java.util.Properties;

public class Test {

    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        prop.setProperty(ClientConfig.LB_TYPE, "static");
        prop.setProperty(ClientConfig.LB_SERVER, "localhost:7101");
        prop.setProperty(ClientConfig.CLIENT_TYPE, "sync");
        prop.setProperty(ClientConfig.COMPRESSION, "0");
//		prop.setProperty(ClientConfig.CLIENT_TYPE, "async");
//		prop.setProperty(ClientConfig.ASYNC_SENDER_THREADS, "3");
//		prop.setProperty(ClientConfig.ASYNC_BATCH_SIZE, "2");
//		prop.setProperty(ClientConfig.ASYNC_TIMEOUT, "1000");
//		prop.setProperty(ClientConfig.ASYNC_QUEUE_TYPE, "file");
//		prop.setProperty(ClientConfig.ASYNC_FILEQUEUE_PATH, "c:/tmp/queue");

        SuroClient client = new SuroClient(prop);

        int num = 1000000;
        for (int i = 1; i <= num; i++) {
            byte[] bytes = ("message #" + i).getBytes();
            client.send(new Message("document-routing", bytes));
        }

        System.out.println("1.______________lost:" + client.getLostMessageCount() + " pending:" + client.getNumOfPendingMessages() + " sent:" + client.getSentMessageCount());
        Thread.sleep(10000);

        client.shutdown();

        System.out.println("2.______________lost:" + client.getLostMessageCount() + " pending:" + client.getNumOfPendingMessages() + " sent:" + client.getSentMessageCount());
    }
}