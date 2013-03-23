package com.netflix.suro.message;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class TestStringSerDe {
    @Test
    public void test() {
        SerDe serde = new StringSerDe();
        assertEquals(serde.getId(), 0);

        String testString = "teststring";
        byte[] serialized = serde.serialize(testString);
        byte[] original = testString.getBytes();
        assertEquals(serialized.length, original.length);
        for (int i = 0; i < original.length; ++i) {
            assertEquals(serialized[i], original[i]);
        }
        assertEquals(serde.deserialize(testString.getBytes()), testString);
    }
}
