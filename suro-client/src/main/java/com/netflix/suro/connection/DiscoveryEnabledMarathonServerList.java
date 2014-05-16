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

import com.netflix.appinfo.InstanceInfo;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.AbstractServerList;
import com.netflix.niws.loadbalancer.DiscoveryEnabledServer;
import mesosphere.marathon.client.MarathonClient;
import mesosphere.marathon.client.model.v2.GetAppTasksResponse;
import mesosphere.marathon.client.model.v2.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by thinker0 on 2014. 5. 16..
 */
public class DiscoveryEnabledMarathonServerList extends AbstractServerList<DiscoveryEnabledServer> {
    private static final Logger logger = LoggerFactory.getLogger(DiscoveryEnabledMarathonServerList.class);

    private String _marathonURI;
    private String _appName;

    @Override
    public void initWithNiwsConfig(IClientConfig clientConfig) {
        this._marathonURI = (String) clientConfig.getProperty(CommonClientConfigKey.PrimeConnectionsURI);
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

    private List<DiscoveryEnabledServer> obtainServersViaDiscovery() {
        final List<DiscoveryEnabledServer> serverList = new ArrayList<DiscoveryEnabledServer>();

        final URI discoveryClient = URI.create(this._marathonURI);
        if (discoveryClient == null || this._marathonURI.startsWith("http")) {
            logger.warn("discoveryClient Marathon URL={}", this._marathonURI);
            return serverList;
        }
        final GetAppTasksResponse appResponse = MarathonClient.getInstance(_marathonURI).getAppTasks(_appName);
        for (final Task task : appResponse.getTasks()) {
            // TODO hostName IP
            for (final Integer port : task.getPorts()) {
                final InstanceInfo instanceInfo = InstanceInfo.Builder.newBuilder()
                        .setAppName(task.getAppId())
                        .setHostName(task.getHost())
                        .setAppName(task.getHost())
                        .setPort(port)
                        .setStatus(InstanceInfo.InstanceStatus.UP)
                        .build();
                serverList.add(new DiscoveryEnabledServer(instanceInfo, false));
            }
        }
        return serverList;
    }

}
