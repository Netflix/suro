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

package com.netflix.suro.sink.notice;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.util.Pair;

/**
 * Notice interface is used to send a notice from the sink to somewhere.
 * This is a kind of pub-sub module, for example, {@link com.netflix.suro.sink.notice.QueueNotice} is used in
 * communication between LocalFileSink and S3FileSink.
 *
 * @param <E> type of notice
 *
 * @author jbae
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface Notice<E> {
    void init();
    boolean send(E message);
    E recv();
    Pair<String, E> peek();
    void remove(String key);
    String getStat();
}
