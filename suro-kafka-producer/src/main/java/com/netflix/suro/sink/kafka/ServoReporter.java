package com.netflix.suro.sink.kafka;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.servo.monitor.DoubleGauge;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.suro.servo.Servo;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class ServoReporter implements MetricsReporter {
    private static final Logger log = LoggerFactory.getLogger(ServoReporter.class);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(false).setNameFormat("ServoReporter-%d").build());
    private ConcurrentMap<DoubleGauge, KafkaMetric> gauges = new ConcurrentHashMap<>();

    @Override
    public void init(List<KafkaMetric> metrics) {
        for (KafkaMetric metric : metrics) {
            addMetric(metric);
        }
    }

    private void addMetric(KafkaMetric metric) {
        MetricName metricName = metric.metricName();
        MonitorConfig.Builder builder = MonitorConfig.builder(metricName.name())
            .withTag("group", metricName.group());
        for(Map.Entry<String, String> tag : metricName.tags().entrySet()) {
            builder.withTag(tag.getKey(), tag.getValue());
        }
        MonitorConfig monitorConfig = builder.build();
        gauges.put(Servo.getDoubleGauge(monitorConfig), metric);
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        addMetric(metric);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        long millis = TimeUnit.MINUTES.toMillis(1);
        scheduler.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                for (Map.Entry<DoubleGauge, KafkaMetric> e : gauges.entrySet()) {
                    e.getKey().set(e.getValue().value());
                }
            }
        }, millis, millis, TimeUnit.MILLISECONDS);

    }
}
