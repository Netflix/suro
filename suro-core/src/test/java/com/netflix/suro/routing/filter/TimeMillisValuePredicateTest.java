package com.netflix.suro.routing.filter;

import org.joda.time.format.DateTimeFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;

public class TimeMillisValuePredicateTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testEachOperatorWorks() throws Exception {
		String valueFormat = "YYYY-MM-dd HH:mm:ss:SSS"; 
		long now = new Date().getTime();
		
		Object[][] inputs = {
			{  now,		"=", 	now, true}, 
			{now+1, 	">", 	now, true}, 
			{  now, 	">=", 	now, true},
			{now+2, 	">=", 	now, true},
			{now-1, 	"<", 	now, true}, 
			{  now, 	"<=", 	now, true}, 
			{now-1, 	"<=", 	now, true},
			
			// Negative cases
			{now+1, 	"=", 	now, false}, 
			{now-1, 	"=", 	now, false}, 
			{now-1, 	">", 	now, false}, 
			{now-1, 	">=", 	now, false},
			{  now, 	"<", 	now, false}, 
			{now+1, 	"<", 	now, false}, 
			{now+1, 	"<=", 	now, false},
		}; 
		
		for(Object[] input : inputs){
			long inputValue = (Long) input[0];
			String fn = (String)input[1];
			long value = (Long) input[2];
			boolean expected = (Boolean) input[3];
			
			verifyValuesOfDifferentFormatsCanBeCompared(valueFormat, value, inputValue, fn, expected);
		}
	}

	@Test(expected=IllegalArgumentException.class)
	public void testInvalidFunctionNameShouldBeRejected() {
		String valueFormat = "YYYY-MM-dd HH:mm:ss:SSS"; 
		long now = new Date().getTime();
		
		new TimeMillisValuePredicate(valueFormat, toString(now, valueFormat), "~~");
	}

	public void verifyValuesOfDifferentFormatsCanBeCompared (
        String valueFormat,
        long value,
        long input,
        String fnName,
        boolean expectedValue) throws Exception {
		
	    String stringValue = toString(value, valueFormat); 
		
		TimeMillisValuePredicate pred = new TimeMillisValuePredicate(valueFormat, stringValue, fnName);
		
		boolean result = pred.apply(input);
		
		assertEquals(
			String.format(
				"Expected: %s %s %s where value format = %s. Expected string value: %s %s %s ", 
				input, fnName, value, valueFormat, input, fnName, stringValue), 
			expectedValue, 
			result);
    }
	
	private String toString(long millis, String format){
		return DateTimeFormat.forPattern(format).print(millis);
	}
}
