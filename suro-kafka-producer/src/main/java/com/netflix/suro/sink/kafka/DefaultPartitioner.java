package com.netflix.suro.sink.kafka;

import org.apache.kafka.common.PartitionInfo;

import java.util.List;

public class DefaultPartitioner implements Partitioner {

    /**
     * it is ok to return null.
     * we just leave the decision to kafka internal default partitioner
     */
    @Override
    public Integer partition(String topic, byte[] partitionKey, List<PartitionInfo> partitions) {
        return null;
    }

}
