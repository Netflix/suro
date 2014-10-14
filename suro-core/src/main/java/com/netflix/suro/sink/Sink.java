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

package com.netflix.suro.sink;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.suro.message.MessageContainer;

/**
 * Suro Sink interface
 *
 * @author jbae
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface Sink  {
    /**
     * Write a single message into the sink
     * 
     * @param message
     */
    void writeTo(MessageContainer message);

    /**
     * Open the Sink by establishing any network connections and setting up writing threads
     */
    void open();
    
    /**
     * Close connections as part of system shutdown or if a Sink is removed
     */
    void close();
    
    String recvNotice();
    String getStat();

    long getNumOfPendingMessages();
    long checkPause();
}
