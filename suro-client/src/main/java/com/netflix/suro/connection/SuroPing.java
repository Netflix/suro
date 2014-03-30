package com.netflix.suro.connection;

import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.AbstractLoadBalancerPing;
import com.netflix.loadbalancer.Server;

/**
 * Simple implementation for checking if a single server is alive
 *
 * @author zdexter
 *
 */
public class SuroPing extends AbstractLoadBalancerPing {
	private static Logger logger = LoggerFactory
            .getLogger(SuroPing.class);

    public SuroPing() {
    }

    public boolean isAlive(Server server) {
        TSocket socket = new TSocket(server.getHost(), server.getPort(), 2000);
        TFramedTransport transport = new TFramedTransport(socket);

        try {
			transport.open();
			return true;
		} catch (TTransportException e) {
			return false;
		}
    }

    @Override
    public void initWithNiwsConfig(IClientConfig clientConfig) {
    }
}
