package com.netflix.suro.routing.filter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NumericValuePredicateTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testEachOperatorWorks() throws Exception {
		Object[][] inputs = {
			{1,		"=", 	1L, 	true},
			{.3,	"=",	.3,		true},
			{3, 	">", 	2.1, 	true}, 
			{2, 	">=", 	2, 		true},
			{3,		">=", 	2.2f, 	true},
			{4, 	"<", 	5, 		true}, 
			{4, 	"<=", 	4, 		true}, 
			{4, 	"<=", 	5, 		true},
			
			// Negative cases
			{4, 	"=", 	3L, 	false}, 
			{3.3, 	"=", 	3.4f, 	false}, 
			{3, 	">", 	4, 		false}, 
			{3, 	">=",	4,		false},
			{4, 	"<", 	3, 		false}, 
			{4.2, 	"<", 	3.5f, 	false}, 
			{4L, 	"<=", 	3.2, 	false},
		}; 
		
		for(Object[] input : inputs){
			Number inputValue = (Number)input[0];
			String fn = (String)input[1];
			Number value = (Number)input[2];
			boolean expected = (Boolean) input[3];
			
			verifyValuesOfDifferentFormatsCanBeCompared(inputValue, fn, value, expected);
		}
	}

	@Test(expected=IllegalArgumentException.class)
	public void testInvalidFunctionNameShouldBeRejected() {
		
		new NumericValuePredicate(4, "~~");
	}

	public void verifyValuesOfDifferentFormatsCanBeCompared (
        Number input,
        String fnName,
        Number value,
        boolean expectedValue) throws Exception {
		
		NumericValuePredicate pred = new NumericValuePredicate(value, fnName);
		boolean result = pred.apply(input);
		
		assertEquals(
			String.format(
				"Expected: %s %s %s", 
				input, fnName, value), 
			expectedValue, 
			result);
    }
}
