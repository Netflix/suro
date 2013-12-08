package com.netflix.suro.routing.filter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StringValuePredicateTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testValueComparison() throws Exception {
		Object[][] inputs = {
			{"abc", "abc", true},
			{"", "", true}, 
			{"AB", "A", false},
			{null, null, true},
			{null, "", false},
			{"", null, false}
		};
		
		for(Object[] input : inputs){
			String value = (String)input[0];
			String inputValue = (String)input[1];
			boolean expected = ((Boolean)input[2]).booleanValue();
			
			StringValuePredicate pred = new StringValuePredicate(value);
		
			assertEquals(String.format("Given value = %s, and input = %s", value, inputValue), expected, pred.apply(inputValue));
		}
		
	}
}
