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
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.StringSerDe;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestSequenceFileWriter {
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
    public void test() throws IOException {
        String dir = tempDir.newFolder().getAbsolutePath();

        String spec = "{\n" +
                "    \"type\": \"sequence\"\n" +
                "}";
        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        FileWriter writer = mapper.readValue(spec, new TypeReference<FileWriter>() {});
        writer.open(dir);

        assertEquals(writer.getLength(), 0);
        writer.rotate(dir + "testfile0.suro");
        for (int i = 0; i < 10; ++i) {
            writer.writeTo(
                    new Message("routingKey", ("message0" + i).getBytes()));
        }
        System.out.println("length: " + writer.getLength());
        assertEquals(writer.getLength(), 540);

        writer.rotate(dir + "testfile1.suro");
        assertEquals(writer.getLength(), 100); // empty sequence file length
        assertEquals(checkFileContents(dir + "testfile0.suro", "message0"), 10);

        writer.setDone(dir + "testfile0.suro", dir + "testfile0.done");
        assertFalse(new File(dir + "testfile0.suro").exists());
        checkFileContents(dir + "testfile0.done", "message0");

        for (int i = 0; i < 10; ++i) {
            writer.writeTo(
                    new Message("routingKey", ("message1" + i).getBytes()));
        }
        writer.close();
        assertEquals(checkFileContents(dir + "testfile1.suro", "message1"), 10);
    }

    private int checkFileContents(String filePath, String message) throws IOException {
        SequenceFile.Reader r = new SequenceFile.Reader(
                FileSystem.get(new Configuration()),
                new Path(filePath),
                new Configuration());

        Text routingKey = new Text();
        MessageWritable value = new MessageWritable();
        StringSerDe serde = new StringSerDe();

        int i = 0;
        while (r.next(routingKey, value)) {
            assertEquals(routingKey.toString(), "routingKey");
            assertEquals(serde.deserialize(value.getMessage().getPayload()), message + i);
            ++i;
        }
        r.close();

        return i;
    }
}
