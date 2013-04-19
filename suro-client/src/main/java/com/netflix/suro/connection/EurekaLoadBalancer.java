package com.netflix.suro.connection;

import com.google.inject.Inject;
import com.netflix.governator.guice.lazy.LazySingleton;
import com.netflix.loadbalancer.DynamicServerListLoadBalancer;
import com.netflix.loadbalancer.Server;
import com.netflix.niws.loadbalancer.DiscoveryEnabledNIWSServerList;
import com.netflix.suro.ClientConfig;

@LazySingleton
public class EurekaLoadBalancer extends DynamicServerListLoadBalancer {
    private final int port;

    @Inject
    public EurekaLoadBalancer(ClientConfig config) {
        String[] vipAddress_port = config.getLoadBalancerServer().split(":");
        if (vipAddress_port.length != 2) {
            throw new IllegalArgumentException("EurekaLoadBalancer server should be formatted vipAddress:port");
        }

        this.port = Integer.parseInt(vipAddress_port[1]);

        DiscoveryEnabledNIWSServerList serverList = new DiscoveryEnabledNIWSServerList();
        serverList.setVipAddresses(vipAddress_port[0]);
        setServerListImpl(serverList);
    }

    @Override
    public Server chooseServer(Object key) {
        Server server = super.chooseServer(key);
        server.setPort(port);

        return server;
    }
}
