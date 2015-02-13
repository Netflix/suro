package com.netflix.suro.sink.kafka;

import org.apache.kafka.common.PartitionInfo;

import java.util.List;

public interface Partitioner {

    public Integer partition(final String topic, final byte[] partitionKey, final List<PartitionInfo> partitions);

}
