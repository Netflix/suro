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

package com.netflix.suro.server;

import com.netflix.suro.input.InputManager;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestStatusServer {
    @ClassRule
    public static SuroServerExternalResource suroServer = new SuroServerExternalResource();

    @Test
    public void _2_connectionFailureShouldBeDetected() throws Exception {
        suroServer.getInjector().getInstance(InputManager.class).getInput("thrift").shutdown();

        HttpResponse response = runQuery("surohealthcheck");

        assertEquals(500, response.getStatusLine().getStatusCode());
    }

    private HttpResponse runQuery(String path) throws IOException {
        DefaultHttpClient client = new DefaultHttpClient();
        HttpGet httpget = new HttpGet(String.format("http://localhost:%d/%s", suroServer.getStatusPort(), path));

        try{
            return client.execute(httpget);
        } finally{
            client.getConnectionManager().shutdown();
        }
    }

    @Test
    public void _0_healthcheckShouldPassForHealthyServer() throws Exception {
        HttpResponse response = runQuery("surohealthcheck");
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    @Test
    public void _1_testSinkStat() throws IOException {
        HttpResponse response = runQuery("surosinkstat");
        InputStream data = response.getEntity().getContent();
        BufferedReader br = new BufferedReader(new InputStreamReader(data));
        String line = null;
        StringBuilder sb = new StringBuilder();
        try {
            while ((line = br.readLine()) != null) {
                sb.append(line).append('\n');
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // The order of output lines may change due to implementation of the Map instance. Therefore, we should ignore order of output lines
        assertEquals(new HashSet<>(Arrays.asList(sb.toString().trim().split("\\n+"))), new HashSet<>(Arrays.asList("sink1:sink1 open\n\ndefault:default open\n\n".trim().split("\\n+"))));
    }
}
