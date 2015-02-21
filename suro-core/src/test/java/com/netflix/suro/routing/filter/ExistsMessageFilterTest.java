package com.netflix.suro.routing.filter;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExistsMessageFilterTest {
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testValueComparison() throws Exception {
        Object[][] inputs = {
            {new ImmutableMap.Builder<String, Object>().put("abc", "v1").put("f1", "v2").build(), "abc", true},
            {new ImmutableMap.Builder<String, Object>().put("def", "v1").put("f1", "v2").build(), "abc", false}
        };

        for(Object[] input : inputs){
            String value = input[0].toString();
            String field = (String)input[1];
            boolean expected = ((Boolean)input[2]).booleanValue();

            PathExistsMessageFilter pred = new PathExistsMessageFilter(field);

            assertEquals(String.format("Given value = %s, and field = %s", value, field), expected, pred.apply(input));
        }

    }
}
