package com.netflix.suro.sink.remotefile.formatter;

import com.netflix.config.ConfigurationManager;

public class PropertyPrefixFormatter implements PrefixFormatter {
    private final String propertyName;

    public PropertyPrefixFormatter(String propertyName) {
        this.propertyName = propertyName;
    }

    @Override
    public String format() {
        return ConfigurationManager.getConfigInstance().getProperty(propertyName).toString();
    }
}
