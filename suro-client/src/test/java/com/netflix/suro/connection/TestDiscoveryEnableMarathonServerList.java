package com.netflix.suro.connection;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.DefaultClientConfigImpl;
import com.netflix.client.config.IClientConfig;
import com.netflix.niws.loadbalancer.DiscoveryEnabledServer;
import com.netflix.suro.jackson.DefaultObjectMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by thinker0 on 14. 5. 23..
 * @author thinker0
 */
public class TestDiscoveryEnableMarathonServerList {
    private static final Logger logger = LoggerFactory.getLogger(TestDiscoveryEnableMarathonServerList.class);

    @Test
    public void initTest() throws IOException {
        final DefaultObjectMapper objectMapper = mock(DefaultObjectMapper.class);
        final Map<String, Object> returnResult = Maps.newHashMap();
        final List<Integer> ports = Arrays.asList(1111, 2222);
        final Map<String, Object> task = Maps.newHashMap();
        task.put("host", "host1");
        task.put("ports", ports);
        final List<Map<String, Object>> tasks = Lists.newArrayList();
        tasks.add(task);
        tasks.add(task);
        returnResult.put("tasks", tasks);

        final URL discoveryClient = URI.create("http://localhost/v2/apps/suro/tasks").toURL();
        final TypeReference<HashMap<String, Object>> typeReference = new TypeReference<HashMap<String, Object>>() {};
        when(objectMapper.readValue(any(discoveryClient.getClass()), any(typeReference.getClass()))).thenReturn(returnResult);

        final DiscoveryEnabledMarathonServerList marathonServerList = new DiscoveryEnabledMarathonServerList(objectMapper);
        final IClientConfig loadBalancerConfig = new DefaultClientConfigImpl();
        final Map<String, Object> properties = loadBalancerConfig.getProperties();
        properties.put("SuroClient.LoadBalancerServer", "http://localhost/v2/apps/suro/tasks");
        loadBalancerConfig.setProperty(CommonClientConfigKey.AppName, "suro");
        marathonServerList.initWithNiwsConfig(loadBalancerConfig);
        List<DiscoveryEnabledServer> initialListOfServers = marathonServerList.getInitialListOfServers();
        verify(objectMapper).readValue(any(discoveryClient.getClass()), any(typeReference.getClass()));
        assertEquals(4, initialListOfServers.size());
    }


    @Test
    public void initListTest() throws IOException {
        final DefaultObjectMapper origiMapper = new DefaultObjectMapper();
        final URL discoveryClient = getClass().getResource("/test_marathon_tasks.json");
        final DiscoveryEnabledMarathonServerList marathonServerList = new DiscoveryEnabledMarathonServerList(origiMapper);
        final IClientConfig loadBalancerConfig = new DefaultClientConfigImpl();
        final Map<String, Object> properties = loadBalancerConfig.getProperties();
        properties.put("SuroClient.LoadBalancerServer", discoveryClient.toString());
        loadBalancerConfig.setProperty(CommonClientConfigKey.AppName, "suro");
        marathonServerList.initWithNiwsConfig(loadBalancerConfig);
        final List<DiscoveryEnabledServer> initialListOfServers = marathonServerList.getInitialListOfServers();
        assertEquals(5, initialListOfServers.size());
    }

    @Test
    public void initListException() throws IOException {
        final DefaultObjectMapper origiMapper = mock(DefaultObjectMapper.class);
        final URL discoveryClient = getClass().getResource("/test_marathon_tasks.json");
        final TypeReference<HashMap<String, Object>> typeReference = new TypeReference<HashMap<String, Object>>() {};
        when(origiMapper.readValue(any(discoveryClient.getClass()), any(typeReference.getClass()))).thenThrow(new IOException("Error"));

        final DiscoveryEnabledMarathonServerList marathonServerList = new DiscoveryEnabledMarathonServerList(origiMapper);
        final IClientConfig loadBalancerConfig = new DefaultClientConfigImpl();
        final Map<String, Object> properties = loadBalancerConfig.getProperties();
        properties.put("SuroClient.LoadBalancerServer", discoveryClient.toString());
        loadBalancerConfig.setProperty(CommonClientConfigKey.AppName, "suro");
        marathonServerList.initWithNiwsConfig(loadBalancerConfig);
        List<DiscoveryEnabledServer> initialListOfServers = marathonServerList.getInitialListOfServers();
        doThrow(new IOException()).when(origiMapper).readValue(any(discoveryClient.getClass()), any(typeReference.getClass()));
        assertEquals(0, initialListOfServers.size());
    }
}
