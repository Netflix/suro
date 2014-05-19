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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.suro.message.Message;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * {@link LocalFileSink} is using Hadoop file IO module. For the text file, it's
 * using <a href=http://hadoop.apache.org/docs/r1.0.4/api/org/apache/hadoop/fs/FSDataOutputStream.html>FSDataOutputStream</a>
 * and for the binary formatted file, it's using
 * <a href=http://hadoop.apache.org/docs/r1.0.4/api/org/apache/hadoop/io/SequenceFile.html>SequenceFile</a>.
 *
 * @author jbae
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = SequenceFileWriter.TYPE, value = SequenceFileWriter.class),
        @JsonSubTypes.Type(name = TextFileWriter.TYPE, value = TextFileWriter.class)
})
public interface FileWriter {
    /**
     * Open the file under outputDir with the file name formatted by
     * {@link FileNameFormatter}
     *
     * @param outputDir the directory where the file is located
     * @throws java.io.IOException
     */
    void open(String outputDir) throws IOException;

    /**
     * @return the file length
     * @throws java.io.IOException
     */
    long getLength() throws IOException;
    void writeTo(Message message) throws IOException;

    /**
     * Flush all data to the disk
     *
     * @throws java.io.IOException
     */
    void sync() throws IOException;

    /**
     * Close the current file, create and open the new file.
     *
     * @param newPath The path that points to the newly rotated file.
     * @throws java.io.IOException
     */
    void rotate(String newPath) throws IOException;
    FileSystem getFS();

    void close() throws IOException;

    /**
     * Marks the file as done. When the file is marked as done,
     * it can be processed further such as uploading it to S3.
     *
     * @param oldName The name of the file when it is not done.
     * @param newName The new name of the file after is is marked as done.
     * @throws java.io.IOException
     */
    void setDone(String oldName, String newName) throws IOException;
}
