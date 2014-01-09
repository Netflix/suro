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

package com.netflix.suro.client.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.client.SuroClient;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.Message;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SuroClient4Test {
    public static void main(String[] args) throws JsonProcessingException, InterruptedException {
        // ip num_of_messages message_size sleep num_of_iterations
        String ip = args[0];
        int numMessages = Integer.parseInt(args[1]);
        int messageSize = Integer.parseInt(args[2]);
        int sleep = Integer.parseInt(args[3]);
        int numIterations = Integer.parseInt(args[4]);

        Properties props = new Properties();
        props.setProperty(ClientConfig.LB_TYPE, "static");
        props.setProperty(ClientConfig.LB_SERVER, ip);

        SuroClient client = new SuroClient(props);
        byte[] payload = createMessagePayload(messageSize);
        for (int n = 0; n < numIterations; ++n) {
            for (int i = 0; i < numMessages; ++i) {
                client.send(new Message(i % 2 == 0 ? "request_trace" : "nf_errors_log", payload));
            }
            Thread.sleep(sleep);
        }
        client.shutdown();
    }

    public static byte[] createMessagePayload(int length) throws JsonProcessingException {
        Map<String, Object> map = new HashMap<String, Object>();
        int currentLength = 0;
        int index = 0;
        while (currentLength < length) {
            String key = "f" + index;
            String value = "v" + index;
            map.put(key, value);
            currentLength += key.length() + value.length();
            ++index;
        }

        return new DefaultObjectMapper().writeValueAsBytes(map);
    }
}
