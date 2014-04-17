package com.netflix.suro.sink.kafka;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;

public class ZkExternalResource extends ExternalResource {
    private ZkServer    zkServer;
    private ZkClient    zkClient;
    private TemporaryFolder tempDir = new TemporaryFolder();


    @Override
    protected void before() throws Throwable {
        tempDir.create();

        zkServer = startZkServer();

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
            zkServer.shutdown();
        }
        tempDir.delete();
    }


    public ZkServer startZkServer() throws Exception {
        String dataPath = tempDir.newFolder().getAbsolutePath();
        String logPath  = tempDir.newFolder().getAbsolutePath();

        ZkServer zkServer = new ZkServer(
                dataPath,
                logPath,
                new IDefaultNameSpace() {
                    @Override
                    public void createDefaultNameSpace(ZkClient zkClient) {
                    }
                },
                pickPort(),
                ZkServer.DEFAULT_TICK_TIME, 100);
        zkServer.start();
        return zkServer;
    }

    public ZkClient getZkClient() {
        return zkClient;
    }
    public int getServerPort() { return zkServer.getPort(); }

    public int pickPort() throws IOException {
        ServerSocket socket = new ServerSocket(0);
        int port = socket.getLocalPort();
        socket.close();

        return port;
    }
}
