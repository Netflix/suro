package com.netflix.suro.routing.filter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.suro.routing.filter.VerificationUtil.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NotMessageFilterTest {
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testNotTrueIsFalse() {
		assertFalse(new NotMessageFilter(getTrueFilter()).apply(DUMMY_INPUT));
	}
	
	@Test
	public void testNotFalseIsTrue() {
		assertTrue(new NotMessageFilter(getFalseFilter()).apply(DUMMY_INPUT));
	}
}
