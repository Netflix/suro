package com.netflix.suro.sink.kafka;

import org.apache.kafka.common.PartitionInfo;

import java.util.List;

public interface Partitioner {

    Integer partition(final String topic, final byte[] partitionKey, final List<PartitionInfo> partitions);

    boolean reset(final String topic, final int partition);

}
