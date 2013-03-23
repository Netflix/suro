package com.netflix.suro.notify;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.suro.nofify.Notify;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertNull;
import static junit.framework.TestCase.assertEquals;

public class TestQueueNotify {
    @Test
    public void test() throws IOException {
        String desc = "{\n" +
                "    \"type\": \"queue\"   \n" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        Notify queueNotify = mapper.readValue(desc, new TypeReference<Notify>(){});
        queueNotify.send("message");
        assertEquals(queueNotify.recv(), "message");
        assertNull(queueNotify.recv());
    }
}
