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

package com.netflix.suro.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.suro.ClientConfig;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.Message;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SuroClient4Test {
    public static void main(String[] args) throws JsonProcessingException {
        Properties props = new Properties();
        props.setProperty(ClientConfig.LB_TYPE, "static");
        props.setProperty(ClientConfig.LB_SERVER, args[0]);

        SuroClient client = new SuroClient(props);
        byte[] payload = createMessagePayload(Integer.parseInt(args[2]));
        for (int i = 0; i < Integer.parseInt(args[1]); ++i) {
            client.send(new Message(i % 2 == 0 ? "request_trace" : "nf_errors_log", payload));
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
