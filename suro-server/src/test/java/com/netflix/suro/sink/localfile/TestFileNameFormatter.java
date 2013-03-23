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
