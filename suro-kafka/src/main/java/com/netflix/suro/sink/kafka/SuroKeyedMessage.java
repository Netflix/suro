package com.netflix.suro.sink.kafka;

import com.netflix.suro.message.Message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SuroKeyedMessage extends Message {
    static {
        Message.classMap.put((byte) 1, SuroKeyedMessage.class);
    }

    private long key;
    private Message message = new Message();

    public SuroKeyedMessage() {}
    public SuroKeyedMessage(long key, Message message) {
        this.key = key;
        this.message = message;
    }

    @Override
    public String getRoutingKey() {
        return message.getRoutingKey();
    }

    @Override
    public byte[] getPayload() {
        return message.getPayload();
    }

    public long getKey() { return key; }

    @Override
    public String toString() {
        return String.format("routingKey: %s, payload byte size: %d",
                getRoutingKey(),
                getPayload().length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SuroKeyedMessage keyedMessage = (SuroKeyedMessage) o;
        if (key == keyedMessage.key) {
            return message.equals(keyedMessage.message);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return (int) (key * 31 + message.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(key);
        message.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        key = dataInput.readLong();
        message.readFields(dataInput);
    }
}
