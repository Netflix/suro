package com.netflix.suro.routing.filter;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VerificationUtil {
	public static final Object DUMMY_INPUT = new Object();

	private VerificationUtil(){}

	/**
     * Creating mocked filter instead of using AwaysTrueFilter so this test case
     * is independent of other test target.  
     */
    public static MessageFilter getTrueFilter() {
        MessageFilter trueFilter = mock(MessageFilter.class);
    	when(trueFilter.apply(VerificationUtil.DUMMY_INPUT)).thenReturn(true);
        return trueFilter;
    }

	public static MessageFilter getFalseFilter() {
        MessageFilter falseFilter = mock(MessageFilter.class);
    	when(falseFilter.apply(VerificationUtil.DUMMY_INPUT)).thenReturn(false);
        return falseFilter;
    }
}
