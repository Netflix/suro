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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.suro.message.Message;

public class AvroFileWriter implements FileWriter {
	static Logger								log		= LoggerFactory.getLogger(AvroFileWriter.class);

	public static final String					TYPE	= "avro";

	private FSDataOutputStream					fsOutputStream;

	private final FileWriterBase				base;
	private final DatumWriter<GenericRecord>	datumWriter;
	private final Schema						schema;
	private DataFileWriter<GenericRecord>		dataFileWriter;

	@JsonCreator
	public AvroFileWriter(@JsonProperty("schema") String schema) {
		base = new FileWriterBase(null, log, new Configuration());
		this.schema = new Schema.Parser().parse(schema);
		datumWriter = new GenericDatumWriter<GenericRecord>(this.schema);
		log.info("Schema loaded > " + schema);
	}

	@Override
	public void open(String outputDir) throws IOException {
		base.createOutputDir(outputDir);
	}

	@Override
	public long getLength() throws IOException {
		if (fsOutputStream != null) {
			return fsOutputStream.getPos();
		} else {
			return 0;
		}
	}

	@Override
	public void writeTo(Message message) throws IOException {
		byte[] payload = message.getPayload();
		dataFileWriter.appendEncoded(ByteBuffer.wrap(payload));
	}

	@Override
	public void rotate(String newPath) throws IOException {
		close();

		fsOutputStream = base.createFSDataOutputStream(newPath);
		dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.create(schema, fsOutputStream);
	}

	@Override
	public FileSystem getFS() {
		return base.getFS();
	}

	@Override
	public void close() throws IOException {
		if (dataFileWriter != null) {
			dataFileWriter.close();
			fsOutputStream.close();
		}
	}

	@Override
	public void setDone(String oldName, String newName) throws IOException {
		base.setDone(oldName, newName);
	}

	@Override
	public void sync() throws IOException {
		dataFileWriter.fSync();
		fsOutputStream.sync();
	}
}
