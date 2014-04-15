package com.netflix.suro.sink.kafka;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.io.FileUtils;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.UnsupportedEncodingException;

public class ZkExternalResource extends ExternalResource {
    public static final String ZK_SERVER_NAME  = "testzkserver";
    public static final int    ZK_SERVER_PORT  = 2181;

    private  ZkServer    zkServer;
    private  ZkClient    zkClient;

    @Override
    protected void before() throws Throwable {
        zkServer = startZkServer();

        zkClient = new ZkClient("localhost:2181", 20000, 20000, new ZkSerializer() {
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
    }

    public ZkServer startZkServer() throws Exception {
        String dataPath = "./build/test/" + ZK_SERVER_NAME + "/data";
        String logPath  = "./build/test/" + ZK_SERVER_NAME + "/log";
        FileUtils.deleteDirectory(new File(dataPath));
        FileUtils.deleteDirectory(new File(logPath));

        ZkServer zkServer = new ZkServer(
                dataPath,
                logPath,
                new IDefaultNameSpace() {
                    @Override
                    public void createDefaultNameSpace(ZkClient zkClient) {
                    }
                },
                ZK_SERVER_PORT,
                ZkServer.DEFAULT_TICK_TIME, 100);
        zkServer.start();
        return zkServer;
    }

    public ZkClient getZkClient() {
        return zkClient;
    }
}
