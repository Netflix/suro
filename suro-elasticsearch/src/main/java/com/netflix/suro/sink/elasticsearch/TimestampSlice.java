package com.netflix.suro.sink.elasticsearch;

public enum TimestampSlice {
    ts_millisecond {
        String get(long ts) {
            return Long.toString(ts);
        }
    },
    ts_second {
        String get(long ts) {
            return Long.toString(ts / 1000);
        }
    },
    ts_minute {
        String get(long ts) {
            return Long.toString(ts / 60000);
        }
    };

    abstract String get(long ts);
}