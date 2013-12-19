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
package com.netflix.suro.server;

import com.netflix.suro.SuroControl;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 *
 */
public class TestSuroControl {
    private SuroControl server;

    @Before
    public void setup() {
        server = new SuroControl();
    }

    @Test
    public void testOnlyExitCommandWorks() throws Exception {
        final CountDownLatch serverDone = new CountDownLatch(1);

        final AtomicBoolean serverExited = new AtomicBoolean(false);
        final AtomicBoolean serverCrashed = new AtomicBoolean(false);
        final int port = findAvailablePort(2000, Short.MAX_VALUE);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    server.start(port);
                    serverExited.set(true);
                } catch (IOException e) {
                    serverCrashed.set(true);
                } finally {
                    serverDone.countDown();
                }
            }
        }).start();

        Socket client = null;
        int testRetry = 100;
        for (int i = 0; i < testRetry; ++i) {
            try {
                client = new Socket("127.0.0.1", port);
                break;
            } catch (Exception e) {
                Thread.sleep(50);
                if (i == testRetry - 1) {
                    throw new RuntimeException("we tried to create client socket but it fails: " + e.getMessage(), e);
                }
            }
        }

        PrintWriter out = new PrintWriter(client.getOutputStream(), true);
        BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));

        try {
            String unknownCmd = "testtest";
            out.append(unknownCmd);
            out.append("\n");
            out.flush();
            String response = reader.readLine();
            assertTrue(String.format("Expected '%s' in the response, but the response was %s", unknownCmd, response), response.contains(String.format("'%s'", unknownCmd)));
            assertFalse(serverExited.get());
            assertFalse(serverCrashed.get());

            out.append(" exit  \n");
            out.flush();
            response = reader.readLine();
            assertEquals("ok", response);

            serverDone.await(5, TimeUnit.SECONDS);
            assertTrue(serverExited.get());
            assertFalse(serverCrashed.get());
        } finally {
            out.close();
            reader.close();
            client.close();
        }
    }

    private int findAvailablePort(int low, int high) {
        for (int i = low; i < high; ++i) {
            if (isAvailable(i)) {
                return i;
            }
        }

        throw new IllegalStateException(String.format("Can't find any available port between %d and %d", low, high));
    }

    private boolean isAvailable(int port) {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(port);
            socket.setReuseAddress(true);
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    //ignore
                }
            }
        }
    }
}
