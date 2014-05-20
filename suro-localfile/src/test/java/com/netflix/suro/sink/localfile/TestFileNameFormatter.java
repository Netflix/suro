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

package com.netflix.suro.sink.localfile;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class TestFileNameFormatter {
    @Test
    public void test() {
        String name = FileNameFormatter.get("/dir/");
        System.out.println(name);
        assertEquals(name.indexOf("-"), -1);
        assertEquals(name.indexOf(":"), -1);
    }
}
