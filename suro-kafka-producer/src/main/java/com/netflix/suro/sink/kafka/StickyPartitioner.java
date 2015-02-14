package com.netflix.suro.sink.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import rx.Observable;
import rx.functions.Action1;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * copied from schlep implementation
 */
public class StickyPartitioner implements Partitioner {
    static final String NAME = "sticky";

    public static final long DEFAULT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(5);

    public static class Builder {
        private Supplier<Long> interval = Suppliers.ofInstance(DEFAULT_INTERVAL_MS);

        public Builder withInterval(long interval) {
            this.interval = Suppliers.ofInstance(interval);
            return this;
        }

        public Builder withInterval(long interval, TimeUnit units) {
            this.interval = Suppliers.ofInstance(TimeUnit.MILLISECONDS.convert(interval, units));
            return this;
        }

        public Builder withInterval(Supplier<Long> interval) {
            this.interval = interval;
            return this;
        }

        public StickyPartitioner build() {
            return new StickyPartitioner(this);
        }
    }


    public static Builder builder() {
        return new Builder();
    }

    private final Random prng;
    // sticky interval (in milli-seconds) before moving on to next partition
    private final Supplier<Long> interval;
    // index cache for each topic
    private final ConcurrentMap<String, Integer> indexCache;

    private StickyPartitioner(Builder builder) {
        this.prng = new Random();
        this.interval = builder.interval;
        // seed with a random integer
        this.indexCache = new ConcurrentHashMap<String, Integer>();
        // increment index every interval
        Observable.interval(interval.get(), TimeUnit.MILLISECONDS)
                .subscribe(new Action1<Long>() {
                    @Override
                    public void call(Long aLong) {
                        // clear cache and next time will recompute a new index
                        indexCache.clear();
                    }
                });
    }

    @Override
    public Integer partition(final String topic, final byte[] partitionKey, final List<PartitionInfo> partitions) {
        if(topic == null) {
            throw new IllegalArgumentException("topic is null");
        }
        if(partitions.isEmpty()) {
            throw new IllegalArgumentException("no partitions for topic: " + topic);
        }
        final int numPartitions = partitions.size();
        if(partitionKey != null) {
            // hash the key to choose a partition
            return Utils.abs(Utils.murmur2(partitionKey)) % numPartitions;
        } else {
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

    @VisibleForTesting
    public long getInterval() {
        return interval.get();
    }
}
