package com.netflix.suro.connection;

import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.AbstractLoadBalancerPing;
import com.netflix.loadbalancer.Server;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.SocketException;

/**
 * Simple implementation for checking if a single server is alive
 *
 * @author zdexter
 */
public class SuroPing extends AbstractLoadBalancerPing {
    private static Logger logger = LoggerFactory
            .getLogger(SuroPing.class);

    public SuroPing() {
    }

    private void close(@Nullable TTransport transport) {
        if (transport == null) {
            return;
        }
        transport.close();
    }

    public boolean isAlive(Server server) {
        TSocket socket = null;
        TFramedTransport transport = null;
        try {
            socket = new TSocket(server.getHost(), server.getPort(), 2000);
            socket.getSocket().setTcpNoDelay(true);
            socket.getSocket().setKeepAlive(true);
            socket.getSocket().setSoLinger(true, 0);
            transport = new TFramedTransport(socket);
            transport.open();
            return true;
        } catch (TTransportException e) {
            logger.warn("Ping {}", e.getMessage());
            return false;
        } catch (SocketException e) {
            logger.warn("Ping {}", e.getMessage());
            return false;
        } finally {
            close(transport);
            close(socket);
        }
    }

    @Override
    public void initWithNiwsConfig(IClientConfig clientConfig) {
    }
}
