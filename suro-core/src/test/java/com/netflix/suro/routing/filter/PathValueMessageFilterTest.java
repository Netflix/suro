package com.netflix.suro.routing.filter;

import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class PathValueMessageFilterTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testPositiveSelection() {
		MockRequestTrace requestTrace = Mockito.mock(MockRequestTrace.class);
		
		when(requestTrace.getClientInfoHostName()).thenReturn("localhost");
		
		Map<String, Object> clientInfoMap = Maps.newHashMap();
		clientInfoMap.put("clientName", "client");
		clientInfoMap.put("clientId", (long) 10);
		when(requestTrace.getClientInfoMap()).thenReturn(clientInfoMap);
		
		// Test numerical values can be filtered
		MessageFilter filter = new PathValueMessageFilter("//clientInfoMap/clientId", new NumericValuePredicate(5, ">"));
		assertTrue("Filter should return true as the client ID is 10, greater than 5", filter.apply(requestTrace));
		
		// Test string value can be filtered
		filter = new PathValueMessageFilter("//clientInfoMap/clientName", new StringValuePredicate("client"));
		assertTrue("Filter should return true as client name is 'client'", filter.apply(requestTrace));
		
		// Test attribute is supported
		filter = new PathValueMessageFilter("//@clientInfoHostName", new StringValuePredicate("localhost"));
		assertTrue("Filter should return tre as clientInfoHostName is localhost", filter.apply(requestTrace));
	}
	
	@Test
	public void testNonExistentValueShouldBeFiltered(){
		MockRequestTrace requestTrace = Mockito.mock(MockRequestTrace.class);
		
		MessageFilter filter = new PathValueMessageFilter("//whatever", new StringValuePredicate("whatever"));
		assertFalse(filter.apply(requestTrace));
	}
	
	@Test
	public void testNullValueCanBeAccepted(){
		MockRequestTrace requestTrace = Mockito.mock(MockRequestTrace.class);
		
		MessageFilter filter = new PathValueMessageFilter("//whatever", NullValuePredicate.INSTANCE);
		assertTrue(filter.apply(requestTrace));
	}
}
