package com.netflix.suro.sink.localfile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.suro.message.Message;
import com.netflix.suro.message.SerDe;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class TextFileWriter implements FileWriter {
    static Logger log = LoggerFactory.getLogger(TextFileWriter.class);

    public static final String TYPE = "text";
    private static final byte[] newline;
    private static final String utf8 = "UTF-8";
    static {
        try {
            newline = "\n".getBytes(utf8);
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("can't find " + utf8 + " encoding");
        }
    }

    private final FileWriterBase base;
    private FSDataOutputStream fsOutputStream;
    private DataOutputStream outputStream;

    @JsonCreator
    public TextFileWriter(@JsonProperty("codec") String codecClass) {
        base = new FileWriterBase(codecClass, log);
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
    public void writeTo(Message message, SerDe serde) throws IOException {
        String strMessage = (String) serde.deserialize(message.getPayload());
        outputStream.write(strMessage.getBytes());
        outputStream.write(newline);
    }

    @Override
    public void rotate(String newPath) throws IOException {
        close();

        fsOutputStream = base.createFSDataOutputStream(newPath);
        outputStream = base.createDataOutputStream(fsOutputStream);
    }

    @Override
    public void close() throws IOException {
        if (outputStream != null) {
            outputStream.close();
            fsOutputStream.close();
        }
    }

    @Override
    public void setDone(String oldName, String newName) throws IOException {
        base.setDone(oldName, newName);
    }
}
