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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.suro.input.InputManager;
import com.netflix.suro.input.thrift.ServerConfig;
import com.netflix.suro.thrift.ServiceStatus;
import com.netflix.suro.thrift.SuroServer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.net.SocketException;

/**
 * Healthcheck page for suro server
 * It is checking whether connecting to the suro server itself is available
 *
 * @author jbae
 */
@Path("/surohealthcheck")
@Singleton
public class HealthCheck {
    private static final Logger log = LoggerFactory.getLogger(HealthCheck.class);

    private final ServerConfig config;
    private final InputManager inputManager;
    private SuroServer.Client client;

    @Inject
    public HealthCheck(ServerConfig config, InputManager inputManager) throws SocketException, TTransportException {
        this.config = config;
        this.inputManager = inputManager;
    }

    @GET
    @Produces("text/plain")
    public synchronized String get() {
        try {
            if (inputManager.getInput("thrift") != null) {
                if (client == null) {
                    client = getClient("localhost", config.getPort(), 5000);
                }

                ServiceStatus status = client.getStatus();
                if (status != ServiceStatus.ALIVE) {
                    throw new RuntimeException("NOT ALIVE!!!");
                }
            }

            return "SuroServer - OK";
        } catch (Exception e) {
            throw new RuntimeException("NOT ALIVE!!!");
        }
    }

    private SuroServer.Client getClient(String host, int port, int timeout) throws SocketException, TTransportException {
        TSocket socket = new TSocket(host, port, timeout);
        socket.getSocket().setTcpNoDelay(true);
        socket.getSocket().setKeepAlive(true);
        socket.getSocket().setSoLinger(true, 0);
        TTransport transport = new TFramedTransport(socket);
        transport.open();

        TProtocol protocol = new TBinaryProtocol(transport);

        return new SuroServer.Client(protocol);
    }
}