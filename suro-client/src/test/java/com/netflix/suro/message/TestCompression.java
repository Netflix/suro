package com.netflix.suro.message;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class TestCompression {
    @Test
    public void testNoCompression() {
        Compression compression = Compression.NO;
        assertEquals(compression.getId(), 0);

        String testString = "teststring";
        ByteBuffer buffer = ByteBuffer.allocate(testString.length());
        buffer.put(testString.getBytes());
        assertEquals(compression.compress(buffer.array()), buffer.array());
    }

    @Test
    public void testLZF() {
        Compression compression = Compression.LZF;
        assertEquals(compression.getId(), 1);

        Random rand = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4096; ++i) {
            sb.append((char) (rand.nextInt(95) + 32));
        }

        byte[] compressed = compression.compress(sb.toString().getBytes());
        assertNotSame(compressed, sb.toString().getBytes());
        byte[] decompressed = compression.decompress(compressed);
        assertTrue(Arrays.equals(decompressed, sb.toString().getBytes()));
    }
}
