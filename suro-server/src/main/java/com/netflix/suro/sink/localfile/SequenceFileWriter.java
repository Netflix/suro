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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.suro.message.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SequenceFileWriter implements FileWriter {
    public static final String TYPE = "sequence";

    static Logger log = LoggerFactory.getLogger(SequenceFileWriter.class);

    private final FileWriterBase base;
    private SequenceFile.Writer seqFileWriter;
    private Text routingKey = new Text();

    @JsonCreator
    public SequenceFileWriter(@JsonProperty("codec") String codec) {
        base = new FileWriterBase(codec, log, new Configuration());
    }

    @Override
    public void open(String outputDir) throws IOException {
        base.createOutputDir(outputDir);
    }

    @Override
    public long getLength() {
        if (seqFileWriter != null) {
            try {
                return seqFileWriter.getLength();
            } catch (IOException e) {
                log.error("IOException while getLength: " + e.getMessage());
                return -1;
            }
        } else {
            return 0;
        }
    }

    @Override
    public void writeTo(Message message) throws IOException {
        routingKey.set(message.getRoutingKey());
        seqFileWriter.append(routingKey, new MessageWritable(message));
    }

    @Override
    public void rotate(String newPath) throws IOException {
        if (seqFileWriter != null) {
            seqFileWriter.close();
        }

        seqFileWriter = base.createSequenceFile(newPath);
    }

    @Override
    public FileSystem getFS() {
        return base.getFS();
    }


    @Override
    public void close() throws IOException {
        if (seqFileWriter != null) {
            seqFileWriter.close();
        }
    }

    @Override
    public void setDone(String oldName, String newName) throws IOException {
        base.setDone(oldName, newName);
    }

    @Override
    public void sync() throws IOException {
        seqFileWriter.sync();
    }
}
