/*
 * Copyright 2013 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.suro.connection;

import com.google.inject.Inject;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.DefaultClientConfigImpl;
import com.netflix.client.config.IClientConfig;
import com.netflix.governator.guice.lazy.LazySingleton;
import com.netflix.loadbalancer.DynamicServerListLoadBalancer;
import com.netflix.loadbalancer.Server;
import com.netflix.niws.loadbalancer.DiscoveryEnabledNIWSServerList;
import com.netflix.suro.ClientConfig;

import java.util.List;

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
        IClientConfig loadBalancerConfig = new DefaultClientConfigImpl();
        loadBalancerConfig.loadProperties("suroClient");
        loadBalancerConfig.setProperty(CommonClientConfigKey.DeploymentContextBasedVipAddresses, vipAddress_port[0]);
        loadBalancerConfig.setProperty(CommonClientConfigKey.NIWSServerListClassName, DiscoveryEnabledNIWSServerList.class.getName());
        super.initWithNiwsConfig(loadBalancerConfig);
    }

    @Override
    public Server chooseServer(Object key) {
        Server server = super.chooseServer(key);
        if (server == null) {
            return null;
        }
        server.setPort(port);

        return server;
    }

    @Override
    public List<Server> getServerList(boolean availableOnly) {
        List<Server> serverList = super.getServerList(availableOnly);
        for (Server s : serverList) {
            s.setPort(port);
        }

        return serverList;
    }
}
