package com.netflix.suro.connection;

import com.netflix.loadbalancer.Server;
import com.netflix.suro.SuroServer4Test;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author thinker0
 */
public class TestSuroPing {

    @Test
    public void pingTest() throws Exception {
        final SuroServer4Test server4Test = new SuroServer4Test();
        server4Test.start();
        SuroPing ping = new SuroPing();
        Server server = new Server("localhost", server4Test.getPort());
        assertEquals(true, ping.isAlive(server));
        server4Test.shutdown();
    }

    @Test
    public void pingFailTest() throws Exception {
        SuroPing ping = new SuroPing();
        Server server = new Server("localhost", 7901);
        assertEquals(false, ping.isAlive(server));
    }
}
