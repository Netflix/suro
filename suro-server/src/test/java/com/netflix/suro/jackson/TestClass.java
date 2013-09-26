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

package com.netflix.suro.jackson;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TestClass implements TestInterface {
    @JacksonInject("test")
    private String test;

    private String a;
    private String b;

    @JsonCreator
    public TestClass(
            @JsonProperty("a") String a,
            @JsonProperty("b") @JacksonInject("b") String b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public String getTest() { return test; }
    
    public String getB() { return b; }
}