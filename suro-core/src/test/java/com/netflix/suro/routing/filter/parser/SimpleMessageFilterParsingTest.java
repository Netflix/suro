package com.netflix.suro.routing.filter.parser;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.netflix.suro.routing.filter.MessageFilter;
import com.netflix.suro.routing.filter.MessageFilterCompiler;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.netflix.suro.routing.filter.parser.FilterPredicate.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class SimpleMessageFilterParsingTest {
	private static final String TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss:SSS";
	private final static DateTime now = DateTime.now();
	
	@Parameters
    public static Collection<Object[]> data() {
            return Arrays.asList(
            	new Object[][] {
            		{0, stringComp, "root", "!=", "value", "!value", "value"}, 
            		{1, stringComp, "//a/b/c", "!=", "value", "!value", "value"}, 
            		{2, stringComp, "root", "=", "value", "value", "!value"}, 
            		{3, stringComp, "//a/b/c", "=", "value", "value", "!value"}, 
            		{4, numberComp, "root", "=", -5, -5, 4},
            		{5, numberComp, "//a/b/c/f", "!=", 10.11, 10.12, 10.11},
            		{6, numberComp, "root", ">", 5, 9, 5},
            		{7, numberComp, "root", ">", 5, 9, 4},
            		{8, numberComp, "//a/b/c/f", ">=", 134.12, 134.12, 133},
            		{9, numberComp, "root", ">", 134.12, 150, -100},
            		{10, numberComp, "//a/b/c/f", ">=", 134.12, 140, 100},
            		{11, numberComp, "root", "<", 5, 4, 5},
            		{12, numberComp, "root", "<", 5, 4, 9},
            		{13, numberComp, "//a/b/c/f", "<=", 134.12, 134.12, 190},
            		{14, numberComp, "//a/b/c/f", "<=", 134.12, 100, 190},
            		{15, between, "//a/b/cd", null, new Object[]{6, 10}, 6, 15},
            		{16, between, "//a/b/cd", null, new Object[]{6, 10}, 8, 15},
            		{17, between, "//a/b/cd", null, new Object[]{6, 10}, 8, 10},
            		{18, between, "//a/b/cd", null, new Object[]{6, 10}, 8, 15},
            		{19, between, "//a/b/cd", null, new Object[]{6, 10}, 8, 5},
            		{20, 
            			between, 
            			"//a/b/ef", 
            			null, 
            			new Object[]{
            				timeMillisNHoursAway(-3, TIME_FORMAT), 
            				timeMillisNHoursAway(3, TIME_FORMAT)
            			},
            			nHoursAway(0), 
            			nHoursAway(-4)
            			
            		},
            		{21, 
            			between, 
            			"//a/b/ef", 
            			null, 
            			new Object[]{
            				timeMillisNHoursAway(-3, TIME_FORMAT), 
            				timeMillisNHoursAway(3, TIME_FORMAT)
            			},
            			nHoursAway(-3), 
            			nHoursAway(3)
            			
            		}, 
            		{22, 
            			between, 
            			"//a/b/ef", 
            			null, 
            			new Object[]{
            				timeMillisNHoursAway(-3, TIME_FORMAT), 
            				timeMillisNHoursAway(3, TIME_FORMAT)
            			},
            			nHoursAway(2), 
            			nHoursAway(4)
            		}, 
            		{23, 
            			between, 
            			"//a/b/ef", 
            			null, 
            			new Object[]{
            				timeStringNHoursAway(-3, TIME_FORMAT), 
            				timeStringNHoursAway(3, TIME_FORMAT)
            			},
            			nHoursAway(0, TIME_FORMAT), 
            			nHoursAway(-4, TIME_FORMAT)
            			
            		},
            		{24, 
            			between, 
            			"//a/b/ef", 
            			null, 
            			new Object[]{
            				timeStringNHoursAway(-3, TIME_FORMAT), 
            				timeStringNHoursAway(3, TIME_FORMAT)
            			},
            			nHoursAway(-3, TIME_FORMAT), 
            			nHoursAway(3, TIME_FORMAT)
            			
            		}, 
            		{25, 
            			between, 
            			"//a/b/ef", 
            			null, 
            			new Object[]{
            				timeStringNHoursAway(-3, TIME_FORMAT), 
            				timeStringNHoursAway(3, TIME_FORMAT)
            			},
            			nHoursAway(2, TIME_FORMAT), 
            			nHoursAway(4, TIME_FORMAT)
            		}, 
            		{26, 
            			timeComp, 
            			"//a/b/ef", 
            			">", 
            			timeStringNHoursAway(-3, TIME_FORMAT),
            			nHoursAway(0, TIME_FORMAT), 
            			nHoursAway(-4, TIME_FORMAT)
            			
            		},
            		{27, 
            			timeComp, 
            			"//a/b/ef", 
            			"<", 
            			timeStringNHoursAway(3, TIME_FORMAT),
            			nHoursAway(0, TIME_FORMAT), 
            			nHoursAway(4, TIME_FORMAT)
            		}, 
            		{28, 
            			timeComp, 
            			"//a/b/ef", 
            			"=", 
            			timeStringNHoursAway(3, TIME_FORMAT),
            			nHoursAway(3, TIME_FORMAT), 
            			nHoursAway(4, TIME_FORMAT)
            		}, 
            		{29, 
            			timeComp, 
            			"//a/b/ef", 
            			"!=", 
            			timeStringNHoursAway(3, TIME_FORMAT),
            			nHoursAway(4, TIME_FORMAT), 
            			nHoursAway(3, TIME_FORMAT)
            		}, 
            		{30, 
            			timeComp, 
            			"//a/b/ef", 
            			"<", 
            			timeMillisNHoursAway(3, TIME_FORMAT),
            			nHoursAway(0), 
            			nHoursAway(4)
            		},
            		{31, 
            			timeComp, 
            			"//a/b/ef", 
            			">", 
            			timeMillisNHoursAway(-3, TIME_FORMAT),
            			nHoursAway(0), 
            			nHoursAway(-4)
            			
            		},
            		{32, 
            			timeComp, 
            			"//a/b/ef", 
            			"<", 
            			timeMillisNHoursAway(3, TIME_FORMAT),
            			nHoursAway(0), 
            			nHoursAway(4)
            		}, 
            		{33, 
            			timeComp, 
            			"//a/b/ef", 
            			"=", 
            			timeMillisNHoursAway(3, TIME_FORMAT),
            			nHoursAway(3), 
            			nHoursAway(4)
            		}, 
            		{34, 
            			timeComp, 
            			"//a/b/ef", 
            			"!=", 
            			timeMillisNHoursAway(3, TIME_FORMAT),
            			nHoursAway(0), 
            			nHoursAway(3)
            		}, 
            		{35, isNull, "//a/b/c", null, null, null, "not null value"}, 
            		{36, regex, "//a/b/c", null, "[0-9]+", "1234234", "aaaaaaaaaa"},
            		{37, existsRight, "//a/b/c", "some value", null, "value", "whaever"},
            		{38, existsLeft, "//a/b/c", "some value", null, "value", "whaever"},
            		{39, trueValue, "//a/b/c", null, null, null, null},
            		{40, falseValue, "//a/b", null, null, null, null}, 
            		{41, 
            			inPred, 
            			"//a/b/c", 
            			null, 
            			new Object[]{
            				wrap("abc"), 
            				wrap("def"), 
            				wrap("foo"), 
            				wrap("123"),
            				wrap("end")
            			}, 
            			"foo", 
            			"bar"
            		}, 
            		{41, 
            			inPred, 
            			"//a/b/c", 
            			null, 
            			new Object[]{
            				123, 
            				455, 
            				1234.23,
            				-1324,
            				23.0
            			}, 
            			23.0, 
            			100
            		}
            	}
            );
    }
    
    private static String wrap(String value){
    	return String.format("\"%s\"", value);
    }
    private static long nHoursAway(int n) {
    	return now.plusHours(n).getMillis();
    }
    
    private static String nHoursAway(int n, String format) {
    	return DateTimeFormat.forPattern(format).print(nHoursAway(n));
    }
    
    // Creates a time-millis that is n hours away from now
    private static String timeMillisNHoursAway(int n, String format) {
    	return String.format(
    		"time-millis(\"%s\", \"%s\")", 
    		format, 
    		nHoursAway(n, format));
    }
    
    private static String timeStringNHoursAway(int n, String format) {
    	return String.format(
    		"time-string(\"%s\", \"%s\", \"%s\")", 
    		format,
    		format, 
    		nHoursAway(n, format));
    }
	@Before
	public void setUp() throws Exception {
		
	}
	
	private int index; 
	private FilterPredicate predicate;
	private String predicateString;
	private Object expectedValue;
	private Object unexpectedValue;
	private String xpath;

	public SimpleMessageFilterParsingTest(
		int index, 
		FilterPredicate targetPredicate, 
		String xpath, 
		String operator, 
		Object value, 
		Object expected, 
		Object unexpected
	) {
		this.index = index;
		this.predicate = targetPredicate;
		this.xpath = xpath;
		this.predicateString = targetPredicate.create(xpath, operator, value);
		System.out.println(String.format("[%d] filter = %s", index, predicateString));
		this.expectedValue = expected;
		this.unexpectedValue = unexpected;
	}
	
	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testPredicate() throws Exception {
		try{
			MessageFilter filter = MessageFilterCompiler.compile(predicateString);
			assertNotNull(filter);
			if(predicate != falseValue){
				testPositiveMatch(filter);
			}
			
			if(predicate == trueValue){
				return;
			} 
			
			if(predicate == existsLeft || predicate == existsRight) {
				testNegativeExistsPredicate(filter);
			} else {
				testNegativeMatch(filter);
			}
		}catch(Exception e) {
			System.err.println(String.format("Failed input for test data [%d]: %s", index, predicateString));
			throw e;
		}
		
	}

	private void testNegativeExistsPredicate(MessageFilter filter) {
	    Object unexpectedEvent = createEvent("nonpath", unexpectedValue);
	    assertEquals(
	    	String.format("The object is %s, should not match the filter %s for data set [%d]. Translated filter: %s", 
	    		unexpectedEvent, predicateString, index, filter), 
	    	false, 
	    	filter.apply(unexpectedEvent));
    }

	private void testNegativeMatch(MessageFilter filter) {
	    Object unexpectedEvent = createEvent(xpath, unexpectedValue);
	    assertEquals(
	    	String.format("The object is %s, should not match the filter %s for data set [%d]. Translated filter: %s", 
	    		unexpectedEvent, predicateString, index, filter), 
	    	false, 
	    	filter.apply(unexpectedEvent));
    }

	private void testPositiveMatch(MessageFilter filter) {
	    Object expectedEvent = createEvent(xpath, expectedValue);
	    assertEquals(
	    	String.format("The object is %s, should match the filter %s for data set [%d]. Translated filter: %s ", 
	    		expectedEvent, predicateString, index, filter), 
	    	true, 
	    	filter.apply(expectedEvent));
    }
	
	private static Object createEvent(String path, Object value) {
		List<String> steps = ImmutableList
				.copyOf(Splitter.on('/')
				.trimResults()
				.omitEmptyStrings()
				.split(path));
		
		return createObjectWithPathAndValue(steps, value);
	}
	// Bypass JXPathContext#createPathAndValue() because it doesn't support context-dependent predicates
	private static Map<String, ? extends Object> createObjectWithPathAndValue(List<String> path, Object value) {
		if(path.isEmpty()) {
			throw new IllegalArgumentException("There should be at least one step in the given path");
		}
		
		if(path.size() == 1) {
			// We want to allow null values, so we don't use ImmutableMap.of() here.
			Map<String, ? super Object> map = Maps.newHashMap();
			map.put(path.get(0), value);
			return map;
		}
		
		Map<String, ? extends Object> child = createObjectWithPathAndValue(path.subList(1, path.size()), value);
		return ImmutableMap.of(path.get(0), child);
	}
}
