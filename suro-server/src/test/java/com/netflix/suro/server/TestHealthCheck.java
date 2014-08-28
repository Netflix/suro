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
import com.netflix.suro.input.SuroInput;
import com.netflix.suro.input.thrift.ServerConfig;
import org.apache.thrift.transport.TTransportException;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestHealthCheck {
    @Rule
    public SuroServerExternalResource suroServer = new SuroServerExternalResource();

    @Test
    public void test() throws TTransportException, IOException {
        InputManager inputManager = mock(InputManager.class);
        doReturn(mock(SuroInput.class)).when(inputManager).getInput("thrift");
        HealthCheck healthCheck = new HealthCheck(new ServerConfig() {
            @Override
            public int getPort() {
                return suroServer.getServerPort();
            }
        }, inputManager);
        healthCheck.get();

        suroServer.getInjector().getInstance(InputManager.class).getInput("thrift").shutdown();

        try {
            healthCheck.get();
            fail("exception should be thrown");
        } catch (RuntimeException e) {
            assertEquals(e.getMessage(), "NOT ALIVE!!!");
        }

    }
}
