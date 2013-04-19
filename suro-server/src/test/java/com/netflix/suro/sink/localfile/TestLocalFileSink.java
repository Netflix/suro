package com.netflix.suro.sink.localfile;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.serde.StringSerDe;
import com.netflix.suro.queue.MessageQueue;
import com.netflix.suro.queue.QueueManager;
import com.netflix.suro.sink.Sink;
import com.netflix.suro.thrift.Result;
import com.netflix.suro.thrift.ServiceStatus;
import com.netflix.suro.thrift.TMessageSet;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestLocalFileSink {
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
                "    \"minPercentFreeDisk\": 50,\n" +
                "    \"rotationPeriod\": \"PT5s\",\n" +
                "    \"notify\": {\n" +
                "        \"type\": \"queue\"\n" +
                "    }\n" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        mapper.setInjectableValues(new InjectableValues() {
            @Override
            public Object findInjectableValue(Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance) {
                if (valueId.equals("queueManager")) {
                    return new QueueManager();
                } else {
                    return null;
                }
            }
        });
        Sink sink = mapper.readValue(localFileSinkSpec, new TypeReference<Sink>(){});
        sink.open();
        assertNull(sink.recvNotify());

        for (int i = 0; i < 100; ++i) {
            sink.writeTo(
                    new Message("routingKey", ("message0" + i).getBytes()),
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

    public static class TestMessageQueue extends MessageQueue {
        @Override
        public TMessageSet poll(long timeout, TimeUnit unit) {
            return null;
        }

        @Override
        public Result process(TMessageSet messageSet) throws TException {
            return null;
        }

        @Override
        public long shutdown() throws TException {
            return 0;
        }

        @Override
        public String getName() throws TException {
            return null;
        }

        @Override
        public String getVersion() throws TException {
            return null;
        }
    }

    @Test
    public void testSpaceChecker() throws Exception {
        final String localFileSinkSpec = "{\n" +
                "    \"type\": \"LocalFileSink\",\n" +
                "    \"name\": \"local\",\n" +
                "    \"outputDir\": \"" + TestTextFileWriter.dir + "\",\n" +
                "    \"writer\": {\n" +
                "        \"type\": \"text\"\n" +
                "    },\n" +
                "    \"maxFileSize\": 10240,\n" +
                "    \"minPercentFreeDisk\": 50,\n" +
                "    \"rotationPeriod\": \"PT1m\",\n" +
                "    \"notify\": {\n" +
                "        \"type\": \"queue\"\n" +
                "    }\n" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        final TestMessageQueue queue = new TestMessageQueue();
        final QueueManager queueManager = new QueueManager();
        queueManager.registerService(queue);

        final LocalFileSink.SpaceChecker spaceChecker = mock(LocalFileSink.SpaceChecker.class);
        when(spaceChecker.hasEnoughSpace()).thenReturn(false);

        mapper.setInjectableValues(new InjectableValues() {
            @Override
            public Object findInjectableValue(Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance) {
                if (valueId.equals("queueManager")) {
                    return queueManager;
                } else if (valueId.equals("spaceChecker")) {
                    return spaceChecker;
                } else {
                    return null;
                }
            }
        });
        assertEquals(queue.getStatus(), ServiceStatus.ALIVE);
        assertEquals(queueManager.getStatus(), QueueManager.OK);

        Sink sink = mapper.readValue(localFileSinkSpec, new TypeReference<Sink>(){});
        sink.open();
        assertEquals(queue.getStatus(), ServiceStatus.WARNING);
        assertEquals(queueManager.getStatus(), QueueManager.IN_ERROR);
        assertNull(sink.recvNotify());

        when(spaceChecker.hasEnoughSpace()).thenReturn(true);

        for (int i = 0; i < 100000; ++i) {
            sink.writeTo(
                    new Message("routingKey", ("message0" + i).getBytes()),
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

    @Test
    public void testWithSizeRotation() throws IOException {
        final String localFileSinkSpec = "{\n" +
                "    \"type\": \"LocalFileSink\",\n" +
                "    \"name\": \"local\",\n" +
                "    \"outputDir\": \"" + TestTextFileWriter.dir + "\",\n" +
                "    \"writer\": {\n" +
                "        \"type\": \"text\"\n" +
                "    },\n" +
                "    \"maxFileSize\": 10240,\n" +
                "    \"minPercentFreeDisk\": 50,\n" +
                "    \"rotationPeriod\": \"PT1m\",\n" +
                "    \"notify\": {\n" +
                "        \"type\": \"queue\"\n" +
                "    }\n" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        mapper.setInjectableValues(new InjectableValues() {
            @Override
            public Object findInjectableValue(Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance) {
                if (valueId.equals("queueManager")) {
                    return new QueueManager();
                } else {
                    return null;
                }

            }
        });
        Sink sink = mapper.readValue(localFileSinkSpec, new TypeReference<Sink>(){});
        sink.open();
        assertNull(sink.recvNotify());

        for (int i = 0; i < 100000; ++i) {
            sink.writeTo(
                    new Message("routingKey", ("message0" + i).getBytes()),
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
