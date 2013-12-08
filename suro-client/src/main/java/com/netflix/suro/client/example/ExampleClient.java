package com.netflix.suro.client.example;

import com.netflix.suro.ClientConfig;
import com.netflix.suro.client.SuroClient;
import com.netflix.suro.message.Message;

import java.util.Properties;

public class ExampleClient {
    public static void main(String[] args) {
        // create the client
        final Properties clientProperties = new Properties();
        clientProperties.setProperty(ClientConfig.LB_TYPE, "static");
        clientProperties.setProperty(ClientConfig.LB_SERVER, args[0]);
        clientProperties.setProperty(ClientConfig.CLIENT_TYPE, args[1]);

        SuroClient client = new SuroClient(clientProperties);

        // send the message
        for (int i = 0; i < Integer.parseInt(args[2]); ++i) {
            client.send(new Message("routingKey", "testMessage".getBytes()));
        }

        client.shutdown();
    }
}
