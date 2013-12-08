package com.netflix.suro.routing.filter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;

public class TimeRangeValuePredicateTest {
	private static final String DEFAULT_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss:SSS";
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test(expected=IllegalArgumentException.class)
	public void testNegativeRangeShouldBeRejected() {
		verifyTimeRangeIsCalculatedCorrectly(DEFAULT_TIME_FORMAT, 10, 9, 20, false);
	}
	
	@Test
	public void testRangeChecks(){
		long time = new Date().getTime();
		// [start, end, input, expected]
		Object[][] inputs = {
			{time, time, time, 		false}, 
			{time, time, time + 1, 	false},
			{time, time, time - 1, 	false},
			
			{time, time + 10, time, 		true}, 
			{time, time + 10, time + 1, 	true},
			{time, time + 10, time + 3, 	true},
			{time, time + 10, time + 9, 	true},
			{time, time + 10, time + 10, 	false},
			{time, time + 10, time + 12,	false},
			{time, time + 10, time - 1, 	false},
			{time, time + 10, time - 10,	false}
		}; 
		
		for(Object[] input: inputs){
			long start = (Long) input[0];
			long end  = (Long) input[1];
			long inputValue = (Long) input[2];
			boolean expected = (Boolean) input[3];
			
			verifyTimeRangeIsCalculatedCorrectly(DEFAULT_TIME_FORMAT, start, end, inputValue, expected);
		}
		
	}

	public void verifyTimeRangeIsCalculatedCorrectly(String timeFormat, long startInstant, long endInstant, long inputValue, boolean expected) {
	    String start = TimeUtil.toString(startInstant, timeFormat);
		String end = TimeUtil.toString(endInstant, timeFormat);
		String input = TimeUtil.toString(inputValue, timeFormat);
		
		TimeRangeValuePredicate pred = new TimeRangeValuePredicate(DEFAULT_TIME_FORMAT, start, end);
		boolean result = pred.apply(inputValue);

		String originalValue = String.format("{input = %s, start = %s, end = %s}", inputValue, startInstant, endInstant);
		if(result){
			assertEquals(String.format("The input value %s is in the given range [%s, %s). Original Values: %s", input, start, end, originalValue), expected, result);
		}else{
			assertEquals(String.format("The input value %s is not in the given range [%s, %s). Original Value: %s", input, start, end, originalValue), expected, result);
		}
    }
}
