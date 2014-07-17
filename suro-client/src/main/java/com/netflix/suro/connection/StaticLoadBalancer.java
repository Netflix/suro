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
import com.netflix.client.config.DefaultClientConfigImpl;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.IClientConfig;
import com.netflix.governator.guice.lazy.LazySingleton;
import com.netflix.loadbalancer.BaseLoadBalancer;
import com.netflix.loadbalancer.Server;
import com.netflix.suro.ClientConfig;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;

/**
 * A load balancer that works on a static list of servers as defined in client configuration.
 *
 * @author jbae
 */
@LazySingleton
public class StaticLoadBalancer extends BaseLoadBalancer {

    /**
     * @param config contains the server list, comma separated with the format
     *               hostname:port
     */
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

        IClientConfig loadBalancerConfig = new DefaultClientConfigImpl();
        loadBalancerConfig.loadProperties("suroClient");
		loadBalancerConfig.setProperty(CommonClientConfigKey.NFLoadBalancerPingClassName, "com.netflix.suro.connection.SuroPing");
        super.initWithNiwsConfig(loadBalancerConfig);
        addServers(serverList);
    }

    @PreDestroy
    public void clear() {
        cancelPingTask();
        setServersList(new ArrayList<Server>());
    }
}
