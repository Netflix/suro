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
};