package com.netflix.suro.routing.filter;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.netflix.suro.routing.filter.VerificationUtil.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class OrMessageFilterTest {
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testAllRejectionsLeadToRejection() {
		List<? extends MessageFilter> filters = ImmutableList.of(getFalseFilter(), getFalseFilter(), getFalseFilter());

		OrMessageFilter filter = new OrMessageFilter(filters);
		assertFalse(filter.apply(DUMMY_INPUT));
	}

	@Test
	public void testOneAcceptanceLeadsToAcceptance() {
		List<? extends MessageFilter> filters = ImmutableList.of(getFalseFilter(), getTrueFilter(), getFalseFilter());

		OrMessageFilter filter = new OrMessageFilter(filters);
		assertTrue(filter.apply(DUMMY_INPUT));
	}

	@Test
	public void testOrMessageFilterShortcuts() {
		MessageFilter falseFilter = getFalseFilter();

		MessageFilter trueFilter = getTrueFilter();

		List<? extends MessageFilter> filters = ImmutableList.of(trueFilter, falseFilter);

		assertTrue(new OrMessageFilter(filters).apply(DUMMY_INPUT));
		verify(falseFilter, never()).apply(DUMMY_INPUT);
	}
}
