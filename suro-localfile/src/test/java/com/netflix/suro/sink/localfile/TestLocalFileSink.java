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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.suro.connection.TestConnectionPool;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.MessageSetReader;
import com.netflix.suro.message.StringMessage;
import com.netflix.suro.sink.Sink;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestLocalFileSink {
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    private static Injector injector = Guice.createInjector(
        new SuroSinkPlugin(),
        new AbstractModule() {
            @Override
            protected void configure() {
                bind(ObjectMapper.class).to(DefaultObjectMapper.class);
            }
            }
    );

    @Test
    public void testDefaultParameters() throws IOException {
        String testdir = tempDir.newFolder().getAbsolutePath();

        final String localFileSinkSpec = "{\n" +
                "    \"type\": \"" + LocalFileSink.TYPE + "\",\n" +
                "    \"outputDir\": \"" + testdir + "\"\n" +
                "    }\n" +
                "}";

        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        Sink sink = mapper.readValue(localFileSinkSpec, new TypeReference<Sink>(){});
        sink.open();

        assertNull(sink.recvNotice());

        for (Message m : new MessageSetReader(TestConnectionPool.createMessageSet(10000))) {
            sink.writeTo(new StringMessage(m));
        }

        sink.close();

        System.out.println(sink.getStat());

        int count = 0;
        File dir = new File(testdir);
        File[] files = dir.listFiles();
        for (File file : files) {
            assertTrue(file.getName().contains(".done"));
            if (!file.getName().contains("crc")) {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = null;
                while ((line = br.readLine()) != null) {
                    assertTrue(line.contains("testMessage"));
                    ++count;
                }
                br.close();
            }
        }
        assertEquals(count, 10000);
    }

    @Test
    public void testWithPeriodRotation() throws IOException, InterruptedException {
        String testdir = tempDir.newFolder().getAbsolutePath();

        final String localFileSinkSpec = "{\n" +
                "    \"type\": \"" + LocalFileSink.TYPE + "\",\n" +
                "    \"outputDir\": \"" + testdir + "\",\n" +
                "    \"writer\": {\n" +
                "        \"type\": \"text\"\n" +
                "    },\n" +
                "    \"maxFileSize\": 100000000,\n" +
                "    \"minPercentFreeDisk\": 50,\n" +
                "    \"rotationPeriod\": \"PT5s\",\n" +
                "    \"notice\": {\n" +
                "        \"type\": \"queue\"\n" +
                "    }\n" +
                "}";

        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        Sink sink = mapper.readValue(localFileSinkSpec, new TypeReference<Sink>(){});
        sink.open();
        assertNull(sink.recvNotice());

        for (Message m : new MessageSetReader(TestConnectionPool.createMessageSet(100))) {
            sink.writeTo(new StringMessage(m));
            Thread.sleep(100);
        }

        sink.close();

        int count = 0;
        int fileCount = 0;
        File dir = new File(testdir);
        File[] files = dir.listFiles();
        for (File file : files) {
            assertTrue(file.getName().contains(".done"));
            if (!file.getName().contains("crc")) {
                ++fileCount;
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = null;
                while ((line = br.readLine()) != null) {
                    assertTrue(line.contains("testMessage"));
                    ++count;
                }
                br.close();
            }
        }
        assertEquals(count, 100);
        assertTrue(fileCount > 1);
    }

    @Test
    public void testSpaceChecker() throws Exception {
        String testdir = tempDir.newFolder().getAbsolutePath();

        final String localFileSinkSpec = "{\n" +
                "    \"type\": \"" + LocalFileSink.TYPE + "\",\n" +
                "    \"outputDir\": \"" + testdir + "\",\n" +
                "    \"writer\": {\n" +
                "        \"type\": \"text\"\n" +
                "    },\n" +
                "    \"maxFileSize\": 10240,\n" +
                "    \"minPercentFreeDisk\": 50,\n" +
                "    \"rotationPeriod\": \"PT1m\",\n" +
                "    \"notice\": {\n" +
                "        \"type\": \"queue\"\n" +
                "    }\n" +
                "}";

        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);

        final LocalFileSink.SpaceChecker spaceChecker = mock(LocalFileSink.SpaceChecker.class);
        when(spaceChecker.hasEnoughSpace()).thenReturn(false);

        Sink sink = mapper.readValue(localFileSinkSpec, new TypeReference<Sink>(){});
        sink.open();

        Thread.sleep(1000); // wait until thread starts

        assertNull(sink.recvNotice());

        when(spaceChecker.hasEnoughSpace()).thenReturn(true);

        for (Message m : new MessageSetReader(TestConnectionPool.createMessageSet(10000))) {
            sink.writeTo(new StringMessage(m));
        }

        sink.close();

        int count = 0;
        File dir = new File(testdir);
        File[] files = dir.listFiles();
        for (File file : files) {
            assertTrue(file.getName().contains(".done"));
            if (!file.getName().contains("crc")) {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = null;
                while ((line = br.readLine()) != null) {
                    assertTrue(line.contains("testMessage"));
                    ++count;
                }
                br.close();
            }
        }
        assertEquals(count, 10000);
    }

    @Test
    public void testWithSizeRotation() throws IOException {
        String testdir = tempDir.newFolder().getAbsolutePath();

        final String localFileSinkSpec = "{\n" +
                "    \"type\": \"" + LocalFileSink.TYPE + "\",\n" +
                "    \"outputDir\": \"" + testdir + "\",\n" +
                "    \"writer\": {\n" +
                "        \"type\": \"text\"\n" +
                "    },\n" +
                "    \"maxFileSize\": 10240,\n" +
                "    \"minPercentFreeDisk\": 50,\n" +
                "    \"rotationPeriod\": \"PT10m\",\n" +
                "    \"batchSize\": 1,\n" +
                "    \"notice\": {\n" +
                "        \"type\": \"queue\"\n" +
                "    }\n" +
                "}";

        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        Sink sink = mapper.readValue(localFileSinkSpec, new TypeReference<Sink>(){});
        sink.open();
        assertNull(sink.recvNotice());

        for (Message m : new MessageSetReader(TestConnectionPool.createMessageSet(10000))) {
            sink.writeTo(new StringMessage(m));
        }

        sink.close();

        int count = 0;
        int errorCount = 0;
        File dir = new File(testdir);
        File[] files = dir.listFiles();
        for (File file : files) {
            System.out.println(file.getName());
            assertTrue(file.getName().contains(".done"));
            if (!file.getName().contains("crc")) {
                if (file.length() > 12000) {
                    ++errorCount;
                    // last file can be bigger due to flushing
                }
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = null;
                while ((line = br.readLine()) != null) {
                    assertTrue(line.contains("testMessage"));
                    ++count;
                }
                br.close();
            }
        }
        assertEquals(count, 10000);
        assertTrue(errorCount <= 1);
    }

    @Test
    public void rotateEmptyFile() throws IOException, InterruptedException {
        String testdir = tempDir.newFolder().getAbsolutePath();

        final String localFileSinkSpec = "{\n" +
                "    \"type\": \"" + LocalFileSink.TYPE + "\",\n" +
                "    \"outputDir\": \"" + testdir + "\",\n" +
                "    \"writer\": {\n" +
                "        \"type\": \"text\"\n" +
                "    },\n" +
                "    \"maxFileSize\": 100000000,\n" +
                "    \"minPercentFreeDisk\": 50,\n" +
                "    \"rotationPeriod\": \"PT2s\",\n" +
                "    \"notice\": {\n" +
                "        \"type\": \"queue\"\n" +
                "    }\n" +
                "}";

        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        Sink sink = mapper.readValue(localFileSinkSpec, new TypeReference<Sink>(){});
        sink.open();
        assertNull(sink.recvNotice());

        for (Message m : new MessageSetReader(TestConnectionPool.createMessageSet(100))) {
            sink.writeTo(new StringMessage(m));
            Thread.sleep(100);
        }

        Thread.sleep(3000);

        sink.close();

        int count = 0;
        int fileCount = 0;
        File dir = new File(testdir);
        File[] files = dir.listFiles();
        for (File file : files) {
            assertTrue(file.getName().contains(".done"));
            if (!file.getName().contains("crc")) {
                ++fileCount;
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = null;
                while ((line = br.readLine()) != null) {
                    assertTrue(line.contains("testMessage"));
                    ++count;
                }
                br.close();
                assertTrue(count > 0); // should not empty
            }
        }
        assertEquals(count, 100);
        assertTrue(fileCount > 1);
    }

    @Test
    public void testCleanUp() throws IOException, InterruptedException {
        String testdir = tempDir.newFolder().getAbsolutePath();

        // create files
        final int numFiles = 5;
        Set<String> filePathSet = new HashSet<String>();
        for (int i = 0; i < numFiles; ++i) {
            String fileName = "testFile" + i + (i == numFiles - 1 ? LocalFileSink.suffix : LocalFileSink.done);
            File f = new File(testdir, fileName);
            f.createNewFile();
            FileOutputStream o = new FileOutputStream(f);
            o.write(100 /*any data*/);
            o.close();
            if (i != numFiles - 1) {
                filePathSet.add(f.getAbsolutePath());
            }
        }

        final String localFileSinkSpec = "{\n" +
                "    \"type\": \"" + LocalFileSink.TYPE + "\",\n" +
                "    \"rotationPeriod\": \"PT1m\",\n" +
                "    \"outputDir\": \"" + testdir + "\"\n" +
                "    }\n" +
                "}";

        Thread.sleep(3000); // wait until .suro file is expired
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        LocalFileSink sink = (LocalFileSink)mapper.readValue(
                localFileSinkSpec,
                new TypeReference<Sink>(){});
        assertEquals(sink.cleanUp(testdir, false), numFiles -1); // due to empty file wouldn't be clean up

        Set<String> filePathSetResult = new HashSet<String>();
        for (int i = 0; i < numFiles - 1; ++i) {
            filePathSetResult.add(sink.recvNotice());
        }
        assertEquals(filePathSet, filePathSetResult);

        assertEquals(sink.cleanUp(testdir, true), numFiles);
    }

    @Test
    public void testGetFileExt() {
        assertEquals(LocalFileSink.getFileExt("abc.done"), ".done");
        assertNull(LocalFileSink.getFileExt("abcdone"));
        assertNull(LocalFileSink.getFileExt("abcdone."));
    }
}
