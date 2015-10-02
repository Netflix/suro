package org.apache.kafka.common.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.utils.Time;

public class KafkaMetricBuilder {
	
	public static KafkaMetric newKafkaMetric(Object lock, MetricName metricName, Measurable measurable, MetricConfig config, Time time)
	{
		return new KafkaMetric(lock, metricName, measurable, config, time);
	}

}
