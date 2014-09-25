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

import com.netflix.util.Pair;

/**
 * Do nothing for debugging purpose
 *
 * @author jbae
 */
public class NoNotice implements Notice<String> {
    public static final String TYPE = "no";

    @Override
    public void init() {
    }

    @Override
    public boolean send(String message) {
        return true;
    }

    @Override
    public String recv() {
        return null;
    }

    @Override
    public Pair<String, String> peek() {
        return null;
    }

    @Override
    public void remove(String key) {

    }

    @Override
    public String getStat() {
        return "No";
    }
}