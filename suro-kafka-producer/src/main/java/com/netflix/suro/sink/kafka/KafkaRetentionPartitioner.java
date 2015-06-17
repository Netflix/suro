package com.netflix.suro.sink.kafka;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Singleton;
import com.netflix.config.DynamicLongProperty;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Singleton
public class KafkaRetentionPartitioner {
    private static DynamicLongProperty retention = new DynamicLongProperty(
            "kafka.producer.partition.retention", 1000);

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("KafkaRetentionPartitioner-%d").build());
    private final AtomicLong hash = new AtomicLong(new Random().nextInt((int) retention.get()));

    public KafkaRetentionPartitioner() {
        scheduler.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                hash.incrementAndGet();
            }
        }, retention.get(), retention.get(), TimeUnit.MILLISECONDS);
    }

    public long getKey() {
        return hash.get();
    }
}
