package com.netflix.suro;

import java.io.IOException;
import java.net.ServerSocket;

public class TestUtils {
    public static int pickPort() throws IOException {
        ServerSocket socket = new ServerSocket(0);
        int port = socket.getLocalPort();
        socket.close();

        return port;
    }
}
