package com.netflix.suro.sink.localfile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.serde.SerDe;
import org.apache.hadoop.io.SequenceFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SequenceFileWriter implements FileWriter {
    public static final String TYPE = "sequence";

    static Logger log = LoggerFactory.getLogger(SequenceFileWriter.class);

    private final FileWriterBase base;
    private SequenceFile.Writer seqFileWriter;

    @JsonCreator
    public SequenceFileWriter(@JsonProperty("codec") String codec) {
        base = new FileWriterBase(codec, log);
    }

    @Override
    public void open(String outputDir) throws IOException {
        base.createOutputDir(outputDir);
    }

    @Override
    public long getLength() {
        try {
            return seqFileWriter.getLength();
        } catch (IOException e) {
            log.error("IOException while getLength: " + e.getMessage());
            return -1;
        }
    }

    @Override
    public void writeTo(Message message, SerDe serde) throws IOException {
        throw new UnsupportedOperationException("should be implemented");
    }

    @Override
    public void rotate(String newPath) throws IOException {
        if (seqFileWriter != null) {
            seqFileWriter.close();
        }

        seqFileWriter = base.createSequenceFile(newPath);
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
}
