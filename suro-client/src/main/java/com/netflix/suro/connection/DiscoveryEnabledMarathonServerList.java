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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.AbstractServerList;
import com.netflix.niws.loadbalancer.DiscoveryEnabledServer;
import com.netflix.suro.jackson.DefaultObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by thinker0 on 2014. 5. 16..
 * @author thinker0
 */

public class DiscoveryEnabledMarathonServerList extends AbstractServerList<DiscoveryEnabledServer> {
    private static final Logger logger = LoggerFactory.getLogger(DiscoveryEnabledMarathonServerList.class);

    private String _marathonURI;
    private String _appName;
    private final DefaultObjectMapper _objectMapper;

    public DiscoveryEnabledMarathonServerList() {
        this(new DefaultObjectMapper());
    }

    public DiscoveryEnabledMarathonServerList(DefaultObjectMapper objectMapper) {
        this._objectMapper = objectMapper;
    }

    @Override
    public void initWithNiwsConfig(IClientConfig clientConfig) {
        final Map<String, Object> properties = clientConfig.getProperties();
        this._marathonURI = (String) properties.get("SuroClient.LoadBalancerServer");
        this._appName = (String) clientConfig.getProperty(CommonClientConfigKey.AppName);
    }

    @Override
    public List<DiscoveryEnabledServer> getInitialListOfServers() {
        return obtainServersViaDiscovery();
    }

    @Override
    public List<DiscoveryEnabledServer> getUpdatedListOfServers() {
        return obtainServersViaDiscovery();
    }

    @VisibleForTesting
    private List<DiscoveryEnabledServer> obtainServersViaDiscovery() {
        final List<DiscoveryEnabledServer> serverList = new ArrayList<DiscoveryEnabledServer>();
        final URI discoveryClient = URI.create(this._marathonURI);
        // ex) https://{domainOrIP}/v2/apps/{appName}/tasks/
        try {
            final Map<String, Object> tasks = _objectMapper.readValue(discoveryClient.toURL(),
                    new TypeReference<HashMap<String, Object>>() {});
            if (tasks != null && tasks.containsKey("tasks")) {
                final List<Map<String, Object>> taskList = (List<Map<String, Object>>) tasks.get("tasks");
                for (final Object task : taskList) {
                    if (task instanceof Map) {
                        final String host = (String) ((Map) task).get("host");
                        final List<Integer> ports = (List<Integer>) ((Map) task).get("ports");
                        for (Integer port : ports) {
                            final InstanceInfo instanceInfo = InstanceInfo.Builder.newBuilder()
                                    .setAppName(_appName)
                                    .setHostName(host)
                                    .setAppName(host)
                                    .setPort(port)
                                    .setStatus(InstanceInfo.InstanceStatus.UP)
                                    .build();
                            serverList.add(new DiscoveryEnabledServer(instanceInfo, false));
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
        }
        return serverList;
    }

}
