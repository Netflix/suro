package com.netflix.suro.sink.kafka;

import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.DoubleGauge;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.suro.servo.Servo;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class ServoReporter implements MetricsReporter {
    private volatile Subscription subscription;
    private ConcurrentMap<DoubleGauge, KafkaMetric> gauges = new ConcurrentHashMap<DoubleGauge, KafkaMetric>();

    @Override
    public void init(List<KafkaMetric> metrics) {
        for (KafkaMetric metric : metrics) {
            addMetric(metric);
        }
        // since actual work is just iterating an in-memory map,
        // it should be ok to use the default computation Scheduler
        subscription = Observable.interval(1, TimeUnit.MINUTES)
                .subscribe(new Action1<Long>() {
                    @Override
                    public void call(Long aLong) {
                        for (Map.Entry<DoubleGauge, KafkaMetric> e : gauges.entrySet()) {
                            e.getKey().set(e.getValue().value());
                        }
                    }
                });
    }

    private void addMetric(KafkaMetric metric) {
        MetricName metricName = metric.metricName();
        MonitorConfig.Builder builder = MonitorConfig.builder(metricName.name())
                .withTag("group", metricName.group());
        for(Map.Entry<String, String> tag : metricName.tags().entrySet()) {
            builder.withTag(tag.getKey(), tag.getValue());
        }
        MonitorConfig monitorConfig = builder.build();
        DoubleGauge gauge = new DoubleGauge(monitorConfig);
        KafkaMetric prev = gauges.putIfAbsent(gauge, metric);
        if(prev == null) {
            DefaultMonitorRegistry.getInstance().register(gauge);
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        addMetric(metric);
    }

    @Override
    public void close() {
        if(subscription != null) {
            subscription.unsubscribe();
        }
        gauges.clear();
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
