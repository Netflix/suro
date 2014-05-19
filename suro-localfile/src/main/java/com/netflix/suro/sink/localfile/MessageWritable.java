package com.netflix.suro.sink.localfile;

import com.netflix.suro.message.Message;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MessageWritable implements Writable {
    private final Message m;

    public MessageWritable(Message m) {
        this.m = m;
    }

    public MessageWritable() {
        m = new Message();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        m.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        m.readFields(in);
    }

    public Message getMessage() { return m; }
}
