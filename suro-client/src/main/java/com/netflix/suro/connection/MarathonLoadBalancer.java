/*
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
import com.netflix.suro.ClientConfig;

import java.util.List;
import java.util.Map;

/**
 * Created by thinker0 on 2014. 5. 16..
 * @author thinker0
 */
@LazySingleton
public class MarathonLoadBalancer extends DynamicServerListLoadBalancer {
    private final String _marathonURL;
    private final String _appName;

    /**
     * @param config contains marathonURL
     */
    @Inject
    public MarathonLoadBalancer(ClientConfig config) {
        _marathonURL = config.getLoadBalancerServer();
        _appName = config.getApp();
        if (_marathonURL == null || _marathonURL.startsWith("http")) {
            throw new IllegalArgumentException(String.format(
                    "MarathonLoadBalancer server should be URL ('%s')",
                    config.getLoadBalancerServer()));
        }

        final IClientConfig loadBalancerConfig = new DefaultClientConfigImpl();
        loadBalancerConfig.loadProperties("suroClient");
        loadBalancerConfig.setProperty(CommonClientConfigKey.AppName, _appName);
        Map<String, Object> properties = loadBalancerConfig.getProperties();
        properties.put("SuroClient.LoadBalancerServer", _marathonURL);
        loadBalancerConfig.setProperty(CommonClientConfigKey.NIWSServerListClassName, DiscoveryEnabledMarathonServerList.class.getName());
        super.initWithNiwsConfig(loadBalancerConfig);
    }

    @Override
    public Server chooseServer(Object key) {
        return super.chooseServer(key);
    }

    @Override
    public List<Server> getServerList(boolean availableOnly) {
        return super.getServerList(availableOnly);
    }
}
