package com.netflix.suro.server;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.suro.thrift.ServiceStatus;
import com.netflix.suro.thrift.SuroServer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.net.SocketException;

@Singleton
@Path("/healthcheck")
public class HealthCheck {
    private final ServerConfig config;

    @Inject
    public HealthCheck(ServerConfig config) {
        this.config = config;
    }

    @GET
    @Produces("text/plain")
    public String get() {
        checkConnection("localhost", config.getPort(), 5000);

        return "SuroServer - OK";
    }

    public static void checkConnection(String host, int port, int timeout) {
        TTransport transport = null;

        try {
            transport = openTransport(host, port, timeout);
            openLocalClient(transport);
        } catch (Exception e) {
            throw new RuntimeException("Testing chukwacollector connection failed on port " + port, e);
        } finally {
            closeTransport(transport);
        }
    }

    private static SuroServer.Client openLocalClient(TTransport transport) throws TException {
        TProtocol protocol = new TBinaryProtocol(transport);

        SuroServer.Client client = new SuroServer.Client(protocol);

        ServiceStatus status = client.getStatus();
        if (status != ServiceStatus.ALIVE) {
            throw new RuntimeException("Collector is not alive! -- status:" + status);
        }

        return client;
    }

    private static void closeTransport(TTransport transport) {
        if (transport != null) {
            try {
                transport.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static TTransport openTransport(String host, int port, int timeout) throws SocketException, TTransportException {
        TTransport transport;
        TSocket socket = createTSocket(host, port, timeout);

        transport = new TFramedTransport(socket);
        transport.open();

        return transport;
    }

    private static TSocket createTSocket(String host, int port, int timeout) throws SocketException {
        TSocket socket = new TSocket(host, port, timeout);
        socket.getSocket().setTcpNoDelay(true);
        socket.getSocket().setKeepAlive(true);
        socket.getSocket().setSoLinger(true, 0);
        return socket;
    }
}