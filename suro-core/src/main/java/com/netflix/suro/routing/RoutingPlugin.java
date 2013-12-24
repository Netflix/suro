package com.netflix.suro.routing;

import com.netflix.suro.SuroPlugin;

public class RoutingPlugin extends SuroPlugin {
    @Override
    protected void configure() {
        this.addFilterType(RegexFilter.TYPE, RegexFilter.class);
        this.addFilterType(XPathFilter.TYPE, XPathFilter.class);
    }
}
