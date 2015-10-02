package com.netflix.suro.sink.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.KafkaMetricBuilder;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.SystemTime;
import org.junit.Test;

import com.google.common.util.concurrent.AtomicDouble;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.MonitorRegistry;
import com.netflix.servo.monitor.Monitor;

public class ServoReporterTest {

	@Test
	public void test() {
		ServoReporter sr = new ServoReporter();
		
		List<KafkaMetric> metrics = new ArrayList<KafkaMetric>();
		metrics.add(KafkaMetricBuilder.newKafkaMetric(new Object(), new MetricName("negativeInfinityTest", "group"), new Measurable() {
			
			@Override
			public double measure(MetricConfig config, long now) {
				// TODO Auto-generated method stub
				return Double.NEGATIVE_INFINITY;
			}
		}, new MetricConfig(){}, new org.apache.kafka.common.utils.SystemTime()));
		
		metrics.add(KafkaMetricBuilder.newKafkaMetric(new Object(), new MetricName("magicNumberTest", "group"), new Measurable() {
			
			@Override
			public double measure(MetricConfig config, long now) {
				// TODO Auto-generated method stub
				return 3;
			}
		}, new MetricConfig(){}, new SystemTime()));
		
		sr.init(metrics);
		
		MonitorRegistry mr = DefaultMonitorRegistry.getInstance();
		
		boolean negativityInfitinty=false;
		boolean initialised = false;
		
		Collection<Monitor<?>> coll = mr.getRegisteredMonitors();
		for (Monitor<?> monitor : coll) {
	        if(monitor.getConfig().getName().equals("magicNumberTest"))
	        {
	        	assertEquals(3.0, ((AtomicDouble) monitor.getValue()).get(), 0.0001);
	        	initialised=true;
	        }
	        
	        if(monitor.getConfig().getName().equals("negativeInfinityTest"))
	        {
	        	assertEquals(0.0, ((AtomicDouble) monitor.getValue()).get(), 0.0001);
	        	negativityInfitinty=true;
	        }
        }
		
		assertTrue("Finite Number", negativityInfitinty);
        assertTrue("Initialised stat", initialised);
	}

}
