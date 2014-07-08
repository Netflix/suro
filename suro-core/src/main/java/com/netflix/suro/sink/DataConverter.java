package com.netflix.suro.sink;

import java.util.Map;

public interface DataConverter {
    Map<String, Object> convert(Map<String, Object> msg);
}
