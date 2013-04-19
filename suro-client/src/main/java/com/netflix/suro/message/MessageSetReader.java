package com.netflix.suro.message;

import com.netflix.suro.thrift.TMessageSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class MessageSetReader implements Iterable<Message> {
    static Logger log = LoggerFactory.getLogger(MessageSetReader.class);

    private final TMessageSet messageSet;

    public MessageSetReader(TMessageSet messageSet) {
        this.messageSet = messageSet;

    }

    public String getApp() { return messageSet.getApp(); }
    public String getHostname() { return messageSet.getHostname(); }
    public String getDataType() { return messageSet.getDataType(); }
    public byte getSerDeId() { return messageSet.getSerde(); }

    public boolean checkCRC() {
        long crcReceived = messageSet.getCrc();
        long crc = MessageSetBuilder.getCRC(messageSet.getMessages());

        return crcReceived == crc;
    }

    @Override
    public Iterator<Message> iterator() {
        try {
            final ByteBuffer buffer =
                    ByteBuffer.wrap(Compression.create(
                            messageSet.getCompression()).decompress(messageSet.getMessages()));

            return new Iterator<Message>() {

                @Override
                public boolean hasNext() {
                    return buffer.hasRemaining();
                }

                @Override
                public Message next() {
                    byte[] routingKeyBytes = new byte[buffer.getInt()];
                    buffer.get(routingKeyBytes);
                    String routingKey = new String(routingKeyBytes);

                    byte[] payloadBytes = new byte[buffer.getInt()];
                    buffer.get(payloadBytes);

                    return new Message(routingKey, getApp(), getHostname(), getDataType(), payloadBytes);
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("remove is not supported");
                }
            };
        } catch (Exception e) {
            log.error("Exception while reading: " + e.getMessage(), e);
            return new Iterator<Message>() {
                @Override
                public boolean hasNext() {
                    return false;
                }
                @Override
                public Message next() {
                    return null;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("remove is not supported");
                }
            };
        }
    }
}
