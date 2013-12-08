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

package com.netflix.suro.message.serde;

import com.netflix.suro.message.SerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class SerDeFactory {
    private static final Logger log = LoggerFactory.getLogger(SerDeFactory.class);
    private static ConcurrentMap<String, SerDe> map = new ConcurrentHashMap<String, SerDe>();

    public static SerDe create(String clazz) {
        SerDe serDe = map.get(clazz);
        if (serDe == null) {
            try {
                serDe = (SerDe) Class.forName(clazz).newInstance();
                map.putIfAbsent(clazz, serDe);
            } catch (Exception e) {
                throw new RuntimeException("Exception on creating SerDe using reflection: " + e.getMessage(), e);
            }
        }

        return serDe;
    }
}