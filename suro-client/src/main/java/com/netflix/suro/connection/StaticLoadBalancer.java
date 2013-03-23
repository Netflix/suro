package com.netflix.suro.connection;

import com.netflix.loadbalancer.BaseLoadBalancer;
import com.netflix.loadbalancer.Server;

import java.util.ArrayList;
import java.util.List;

public class StaticLoadBalancer extends BaseLoadBalancer {
    public StaticLoadBalancer(String servers) {
        List<Server> serverList = new ArrayList<Server>();
        for (String s : servers.split(",")) {
            String[] host_port = s.split(":");
            serverList.add(new Server(host_port[0], Integer.parseInt(host_port[1])));
        }

        addServers(serverList);
    }
}
