package com.netflix.suro.routing.filter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NullValuePredicateTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testNullIsAccepted() {
		assertTrue(NullValuePredicate.INSTANCE.apply(null));
	}

	@Test
	public void testNonNullIsRejected() {
		assertFalse(NullValuePredicate.INSTANCE.apply(new Object()));
	}

}
