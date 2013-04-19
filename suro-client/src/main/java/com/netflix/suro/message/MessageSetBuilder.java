package com.netflix.suro.message;

import com.netflix.suro.message.serde.StringSerDe;
import com.netflix.suro.thrift.TMessageSet;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.zip.CRC32;

public class MessageSetBuilder {
    private String hostname;
    private String app = "default";
    private List<Message> messageList;
    private String dataType = "string";
    private Compression compression = Compression.NO;
    private byte serde = StringSerDe.id;

    public MessageSetBuilder() {
        messageList = new LinkedList<Message>();
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "UNKNOWN";
        }
    }

    public MessageSetBuilder withHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public MessageSetBuilder withApp(String app) {
        this.app = app;
        return this;
    }

    public MessageSetBuilder withMessage(String routingKey, byte[] payload) {
        this.messageList.add(new Message(routingKey, payload));
        return this;
    }

    public MessageSetBuilder withSerDe(byte serde) {
        this.serde = serde;
        return this;
    }

    public MessageSetBuilder withCompression(Compression compresson) {
        this.compression = compresson;
        return this;
    }

    public MessageSetBuilder withDatatype(String dataType) {
        this.dataType = dataType;
        return this;
    }

    public TMessageSet build() {
        byte[] buffer = createPayload(messageList, compression);
        long crc = getCRC(buffer);

        messageList.clear();

        return new TMessageSet(
                hostname,
                app,
                dataType,
                serde,
                compression.getId(),
                crc,
                ByteBuffer.wrap(buffer));
    }

    public int size() {
        return messageList.size();
    }

    public static byte[] createPayload(List<Message> messageList, Compression compression) {
        ByteBuffer buffer = ByteBuffer.allocate(getByteSize(messageList));
        for (Message message : messageList) {
            message.writeTo(buffer);
        }
        buffer.rewind();

        return compression.compress(buffer.array());
    }

    public static int getByteSize(List<Message> messageList) {
        int size = 0;
        for (Message message : messageList) {
            size += message.getByteSize();
        }
        return size;
    }

    public static long getCRC(byte[] buffer) {
        CRC32 crc = new CRC32();
        crc.update(buffer);
        return crc.getValue();
    }

    public void drainFrom(BlockingQueue<Message> queue, int size) {
        queue.drainTo(messageList, size);
    }
}
