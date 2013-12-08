package com.netflix.suro.routing.filter;

import com.netflix.suro.routing.filter.RegexValuePredicate.MatchPolicy;
import org.junit.Test;

import java.io.IOException;

import static com.netflix.suro.routing.filter.MessageFilters.*;
import static com.netflix.suro.routing.filter.VerificationUtil.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MessageFiltersTest {
	@Test
	public void testAlwaysFalseReturnsFalse() {
		assertFalse(alwaysFalse().apply(DUMMY_INPUT));
	}

	@Test
	public void testAlwaysTrueReturnsTrue() {
		assertTrue(alwaysTrue().apply(DUMMY_INPUT));
	}
	
	@Test
	public void testNotAlwaysNegates(){
		assertTrue(not(getFalseFilter()).apply(DUMMY_INPUT));
		
		assertFalse(not(getTrueFilter()).apply(DUMMY_INPUT));
	}
	
	@Test
	public void testOr() {
		assertTrue(or(getFalseFilter(), getFalseFilter(), getTrueFilter()).apply(DUMMY_INPUT));
		
		assertFalse(or(getFalseFilter(), getFalseFilter()).apply(DUMMY_INPUT));
	}
	
	@Test
	public void testAnd(){
		assertTrue(and(getTrueFilter(), getTrueFilter()).apply(DUMMY_INPUT));
		
		assertFalse(and(getTrueFilter(), getFalseFilter()).apply(DUMMY_INPUT));
	}
	
	@Test
	public void showQuery() throws Exception {
		MessageFilter filter = MessageFilters.or(
			MessageFilters.and(
				new PathValueMessageFilter("//path/to/property", new StringValuePredicate("foo")),
				new PathValueMessageFilter("//path/to/property", new NumericValuePredicate(123, ">")),
				new PathValueMessageFilter("//path/to/property", new PathValuePredicate("//path/to/property", "//another/path"))
			), 
			MessageFilters.not(
				new PathValueMessageFilter("//path/to/time", new TimeMillisValuePredicate("yyyy-MM-dd", "1997-08-29", "!="))
			), 
			new PathValueMessageFilter("//path/to/stringProp", new RegexValuePredicate(".*", MatchPolicy.PARTIAL)),
			new PathValueMessageFilter("//path/to/stringProp", new RegexValuePredicate(".*", MatchPolicy.FULL))
		);
		
		print(filter);
	}

	private void print(MessageFilter filter) throws IOException {
	    System.out.println(filter.toString());
    }
}
