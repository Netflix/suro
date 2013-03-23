package com.netflix.suro.connection;

import com.netflix.loadbalancer.DynamicServerListLoadBalancer;
import com.netflix.loadbalancer.Server;
import com.netflix.niws.loadbalancer.DiscoveryEnabledNIWSServerList;

public class EurekaLoadBalancer extends DynamicServerListLoadBalancer {
    private final int port;

    public EurekaLoadBalancer(String vipAddress, int port) {
        this.port = port;

        DiscoveryEnabledNIWSServerList serverList = new DiscoveryEnabledNIWSServerList();
        serverList.setVipAddresses(vipAddress);
        setServerListImpl(serverList);
    }

    @Override
    public Server chooseServer(Object key) {
        Server server = super.chooseServer(key);
        server.setPort(port);

        return server;
    }
}
