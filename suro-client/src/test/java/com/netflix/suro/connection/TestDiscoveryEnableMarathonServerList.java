package com.netflix.suro.connection;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.DefaultClientConfigImpl;
import com.netflix.client.config.IClientConfig;
import com.netflix.niws.loadbalancer.DiscoveryEnabledServer;
import com.netflix.suro.jackson.DefaultObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by thinker0 on 14. 5. 23..
 * @author thinker0
 */
public class TestDiscoveryEnableMarathonServerList {

    @Test
    public void initTest() throws IOException {
        final DefaultObjectMapper objectMapper = mock(DefaultObjectMapper.class);
        Map<String, Object> returnResult = Maps.newHashMap();
        final URI discoveryClient = URI.create("http://localhost/v2/apps/suro/tasks");

        when(objectMapper.readValue(discoveryClient.toURL(),
                new TypeReference<HashMap<String, Object>>() {})).thenReturn(returnResult);

        DiscoveryEnabledMarathonServerList marathonServerList = new DiscoveryEnabledMarathonServerList(objectMapper);
        final IClientConfig loadBalancerConfig = new DefaultClientConfigImpl();
        final Map<String, Object> properties = loadBalancerConfig.getProperties();
        properties.put("SuroClient.LoadBalancerServer", "http://localhost/v2/apps/suro/tasks");
        loadBalancerConfig.setProperty(CommonClientConfigKey.AppName, "suro");
        marathonServerList.initWithNiwsConfig(loadBalancerConfig);
        List<DiscoveryEnabledServer> initialListOfServers = marathonServerList.getInitialListOfServers();
        assertEquals(0, initialListOfServers.size());
    }
}
