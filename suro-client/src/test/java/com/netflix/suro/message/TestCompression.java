package com.netflix.suro.message;

import org.junit.Test;

import java.nio.ByteBuffer;

import static junit.framework.Assert.assertEquals;

public class TestCompression {
    @Test
    public void testNoCompression() {
        Compression compression = Compression.NO;
        assertEquals(compression.getId(), 0);

        String testString = "teststring";
        ByteBuffer buffer = ByteBuffer.allocate(testString.length());
        buffer.put(testString.getBytes());
        assertEquals(compression.compress(buffer), buffer);
    }
}
