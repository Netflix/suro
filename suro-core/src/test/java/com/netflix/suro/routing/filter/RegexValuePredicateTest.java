package com.netflix.suro.routing.filter;

import com.netflix.suro.routing.filter.RegexValuePredicate.MatchPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RegexValuePredicateTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testNormalMatch() throws Exception {
		Object[][] inputs = {
			{"a123cd", "a[0-9]+cd", MatchPolicy.FULL, true},
			{"", "",  MatchPolicy.FULL, true}, 
			{"", "^$",  MatchPolicy.FULL, true},
			{"32abde23", "[a-z]+",  MatchPolicy.FULL, false},
			{"32abde23", "[a-z]+", MatchPolicy.PARTIAL, true},
			{null, ".*", MatchPolicy.PARTIAL, false},
			{null, ".*",  MatchPolicy.FULL, false},
			{null, "", MatchPolicy.PARTIAL, false}
		};
		
		for(Object[] input : inputs){
			String value = (String)input[0];
			String regex = (String)input[1];
			MatchPolicy policy = (MatchPolicy)input[2];
			boolean expected = (Boolean) input[3];
			
			RegexValuePredicate pred = new RegexValuePredicate(regex, policy);
		
			assertEquals(
				String.format("Given regex = %s, isPartial = %s, and input = %s", regex, policy, value), 
				expected, 
				pred.apply(value));
		}
		
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNullPatternIsNotAllowed(){
		new RegexValuePredicate(null, MatchPolicy.FULL);
	}
}
