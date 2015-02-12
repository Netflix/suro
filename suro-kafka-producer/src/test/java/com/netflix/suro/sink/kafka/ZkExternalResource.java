package com.netflix.suro.sink.kafka;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.curator.test.TestingServer;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class ZkExternalResource extends ExternalResource {
    private TestingServer zkServer;
    private ZkClient    zkClient;
    private TemporaryFolder tempDir = new TemporaryFolder();


    @Override
    protected void before() throws Throwable {
        tempDir.create();

        zkServer = new TestingServer();

        zkClient = new ZkClient("localhost:" + zkServer.getPort(), 20000, 20000, new ZkSerializer() {
            @Override
            public byte[] serialize(Object data) throws ZkMarshallingError {
                try {
                    return ((String)data).getBytes("UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Object deserialize(byte[] bytes) throws ZkMarshallingError {
                if (bytes == null)
                    return null;
                try {
                    return new String(bytes, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Override
    protected void after() {
        if (zkServer != null) {
            try {
                zkServer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        tempDir.delete();
    }

    public ZkClient getZkClient() {
        return zkClient;
    }
    public int getServerPort() { return zkServer.getPort(); }
    public String getConnectionString() { return zkServer.getConnectString(); }
}
