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

package com.netflix.suro;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.suro.input.InputManager;
import com.netflix.suro.input.thrift.MessageSetProcessor;
import com.netflix.suro.server.StatusServer;
import com.netflix.suro.input.thrift.ThriftServer;
import com.netflix.suro.sink.SinkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Main service for suro to control all subsystems including
 * {@link StatusServer}, {@link ThriftServer}, {@link MessageSetProcessor}, and
 * {@link SinkManager}
 * 
 * @author elandau
 */
@Singleton
public class SuroService {
    static Logger log = LoggerFactory.getLogger(SuroServer.class);

    private final StatusServer statusServer;
    private final InputManager inputManager;
    private final SinkManager  sinkManager;
    
    @Inject
    private SuroService(StatusServer statusServer, InputManager inputManager, SinkManager sinkManager) {
        this.statusServer = statusServer;
        this.inputManager = inputManager;
        this.sinkManager  = sinkManager;
    }

    @PostConstruct
    public void start() {
        try {
            statusServer.start();
            sinkManager.initialStart();
            inputManager.initialStart();
        } catch (Exception e) {
            log.error("Exception while starting up server: " + e.getMessage(), e);
            Throwables.propagate(e);
        }
    }

    @PreDestroy
    public void shutdown() {
        try {
            inputManager.shutdown();
            statusServer.shutdown();
            sinkManager .shutdown();
        } catch (Exception e) {
            //ignore every exception while shutting down but loggign should be done for debugging
            log.error("Exception while shutting down SuroServer: " + e.getMessage(), e);
        }
    }
}
