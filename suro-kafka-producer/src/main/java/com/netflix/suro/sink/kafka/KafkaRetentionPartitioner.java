package com.netflix.suro.sink.kafka;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Singleton;
import com.netflix.config.DynamicLongProperty;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

@Singleton
public class KafkaRetentionPartitioner {
    private final Random prng;
    // index cache for each topic
    private final ConcurrentMap<String, Integer> indexCache;

    private static DynamicLongProperty retention = new DynamicLongProperty(
            "kafka.producer.partition.retention", 1000);

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(false).setNameFormat("KafkaRetentionPartitioner-%d").build());

    public KafkaRetentionPartitioner() {
        scheduler.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                indexCache.clear();
            }
        }, retention.get(), retention.get(), TimeUnit.MILLISECONDS);

        this.prng = new Random();
        // seed with a random integer
        this.indexCache = new ConcurrentHashMap<>();
        // increment index every interval
    }

    public int getKey(String topic, List<PartitionInfo> partitions) {
        if(topic == null) {
            throw new IllegalArgumentException("topic is null");
        }
        if(partitions.isEmpty()) {
            throw new IllegalArgumentException("no partitions for topic: " + topic);
        }
        final int numPartitions = partitions.size();
        Integer index = indexCache.get(topic);
        if(index != null) {
            // stick to the same partition in cache
            return index;
        } else {
            // randomly pick a new partition from [0, numPartitions) range
            int partition = prng.nextInt(numPartitions);
            // try to find a partition with leader
            for (int i = 0; i < numPartitions; i++) {
                if (partitions.get(partition).leader() != null) {
                    // found a partition with leader
                    index = indexCache.putIfAbsent(topic, partition);
                    return index != null ? index : partition;
                } else {
                    // try next partition
                    partition = (partition + 1) % numPartitions;
                }
            }
            // no partitions are available, give a non-available partition.
            // partition will loop back to its earlier value from prng.nextInt(numPartitions).
            // but don't update cache in this case.
            return partition;
        }
    }
}
