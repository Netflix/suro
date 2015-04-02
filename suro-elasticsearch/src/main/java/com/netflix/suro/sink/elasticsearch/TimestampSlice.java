package com.netflix.suro.sink.elasticsearch;

public enum TimestampSlice {
    ts_millisecond {
        public String get(long ts) {
            return Long.toString(ts);
        }
    },
    ts_second {
        public String get(long ts) {
            return Long.toString(ts / 1000);
        }
    },
    ts_minute {
        public String get(long ts) {
            return Long.toString(ts / 60000);
        }
    };

    public abstract String get(long ts);
}