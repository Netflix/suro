package com.netflix.suro.routing.filter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.suro.routing.filter.VerificationUtil.DUMMY_INPUT;
import static org.junit.Assert.assertTrue;

public class AlwaysTrueMessageFilterTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		assertTrue(AlwaysTrueMessageFilter.INSTANCE.apply(DUMMY_INPUT));
	}
}
