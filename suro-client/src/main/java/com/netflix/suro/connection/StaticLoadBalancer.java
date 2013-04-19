package com.netflix.suro.connection;

import com.google.inject.Inject;
import com.netflix.governator.guice.lazy.LazySingleton;
import com.netflix.loadbalancer.BaseLoadBalancer;
import com.netflix.loadbalancer.Server;
import com.netflix.suro.ClientConfig;

import java.util.ArrayList;
import java.util.List;

@LazySingleton
public class StaticLoadBalancer extends BaseLoadBalancer {
    @Inject
    public StaticLoadBalancer(ClientConfig config) {
        List<Server> serverList = new ArrayList<Server>();
        for (String s : config.getLoadBalancerServer().split(",")) {
            String[] host_port = s.split(":");
            serverList.add(new Server(host_port[0], Integer.parseInt(host_port[1])));
        }
        if (serverList.isEmpty()) {
            throw new IllegalArgumentException("empty server list");
        }

        addServers(serverList);
    }
}
