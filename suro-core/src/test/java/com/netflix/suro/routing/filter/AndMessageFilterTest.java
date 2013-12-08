package com.netflix.suro.routing.filter;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class AndMessageFilterTest {
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testAllAcceptancesLeadToAcceptance() {
		List<? extends MessageFilter> filters = ImmutableList.of(
			VerificationUtil.getTrueFilter(), 
			VerificationUtil.getTrueFilter(),
			VerificationUtil.getTrueFilter());
		
		AndMessageFilter filter = new AndMessageFilter(filters);
		assertTrue(filter.apply(VerificationUtil.DUMMY_INPUT));
	}

	@Test
	public void testOneRejectionLeadsToRejection() {
		List<? extends MessageFilter> filters = ImmutableList.of(
			VerificationUtil.getTrueFilter(),
			VerificationUtil.getTrueFilter(),
			VerificationUtil.getFalseFilter()
		);
		
		AndMessageFilter filter = new AndMessageFilter(filters);
		assertFalse(filter.apply(VerificationUtil.DUMMY_INPUT));
	}
	
	@Test
	public void testAndMessageFilterShortcuts() {
		MessageFilter falseFilter = VerificationUtil.getFalseFilter();
		
		MessageFilter trueFilter = VerificationUtil.getTrueFilter();
		
		
		List<? extends MessageFilter> filters = ImmutableList.of(
			falseFilter, trueFilter
		);
		
		assertFalse(new AndMessageFilter(filters).apply(VerificationUtil.DUMMY_INPUT));
		verify(trueFilter, never()).apply(VerificationUtil.DUMMY_INPUT);
	}
}
