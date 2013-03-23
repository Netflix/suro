package com.netflix.suro.sink.localfile;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.StringSerDe;
import com.netflix.suro.sink.Sink;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static junit.framework.Assert.*;

public class TestLocalFileSink {
    public static final String localFileSinkSpec = "{\n" +
            "    \"type\": \"LocalFileSink\",\n" +
            "    \"name\": \"local\",\n" +
            "    \"outputDir\": \"" + TestTextFileWriter.dir + "\",\n" +
            "    \"writer\": {\n" +
            "        \"type\": \"text\"\n" +
            "    },\n" +
            "    \"maxFileSize\": 10240,\n" +
            "    \"rotationPeriod\": \"PT1m\",\n" +
            "    \"notify\": {\n" +
            "        \"type\": \"queue\"\n" +
            "    }\n" +
            "}";

    @Before
    @After
    public void clean() throws IOException {
        TestTextFileWriter.cleanUp();
    }

    @Test
    public void testWithPeriodRotation() throws IOException, InterruptedException {
        final String localFileSinkSpec = "{\n" +
                "    \"type\": \"LocalFileSink\",\n" +
                "    \"name\": \"local\",\n" +
                "    \"outputDir\": \"" + TestTextFileWriter.dir + "\",\n" +
                "    \"writer\": {\n" +
                "        \"type\": \"text\"\n" +
                "    },\n" +
                "    \"maxFileSize\": 100000000,\n" +
                "    \"rotationPeriod\": \"PT5s\",\n" +
                "    \"notify\": {\n" +
                "        \"type\": \"queue\"\n" +
                "    }\n" +
                "}";

        ObjectMapper mapper = new DefaultObjectMapper();
        Sink sink = mapper.readValue(localFileSinkSpec, new TypeReference<Sink>(){});
        sink.open();
        assertNull(sink.recvNotify());

        for (int i = 0; i < 100; ++i) {
            sink.writeTo(
                    new Message("routingKey", "app", "hostname", "datatype", ("message0" + i).getBytes()),
                    new StringSerDe());
            Thread.sleep(100);
        }

        sink.close();

        int count = 0;
        int fileCount = 0;
        File dir = new File(TestTextFileWriter.dir);
        File[] files = dir.listFiles();
        for (File file : files) {
            assertTrue(file.getName().contains(".done"));
            if (file.getName().contains("crc") == false) {
                ++fileCount;
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = null;
                while ((line = br.readLine()) != null) {
                    assertTrue(line.contains("message0"));
                    ++count;
                }
                br.close();
            }
        }
        assertEquals(count, 100);
        assertTrue(fileCount > 1);
    }

    @Test
    public void testWithSizeRotation() throws Exception {
        final String localFileSinkSpec = "{\n" +
                "    \"type\": \"LocalFileSink\",\n" +
                "    \"name\": \"local\",\n" +
                "    \"outputDir\": \"" + TestTextFileWriter.dir + "\",\n" +
                "    \"writer\": {\n" +
                "        \"type\": \"text\"\n" +
                "    },\n" +
                "    \"maxFileSize\": 10240,\n" +
                "    \"rotationPeriod\": \"PT1m\",\n" +
                "    \"notify\": {\n" +
                "        \"type\": \"queue\"\n" +
                "    }\n" +
                "}";

        ObjectMapper mapper = new DefaultObjectMapper();
        Sink sink = mapper.readValue(localFileSinkSpec, new TypeReference<Sink>(){});
        sink.open();
        assertNull(sink.recvNotify());

        for (int i = 0; i < 100000; ++i) {
            sink.writeTo(
                    new Message("routingKey", "app", "hostname", "datatype", ("message0" + i).getBytes()),
                    new StringSerDe());
        }

        sink.close();

        int count = 0;
        File dir = new File(TestTextFileWriter.dir);
        File[] files = dir.listFiles();
        for (File file : files) {
            assertTrue(file.getName().contains(".done"));
            if (file.getName().contains("crc") == false) {
                assertTrue(file.length() < 12000);
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = null;
                while ((line = br.readLine()) != null) {
                    assertTrue(line.contains("message0"));
                    ++count;
                }
                br.close();
            }
        }
        assertEquals(count, 100000);
    }
}
