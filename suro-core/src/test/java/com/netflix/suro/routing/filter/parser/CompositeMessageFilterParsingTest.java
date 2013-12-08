package com.netflix.suro.routing.filter.parser;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.netflix.suro.routing.filter.MessageFilter;
import com.netflix.suro.routing.filter.MessageFilterCompiler;
import com.netflix.suro.routing.filter.lang.InvalidFilterException;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
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

@RunWith(Parameterized.class)
public class CompositeMessageFilterParsingTest {
	private static final String TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss:SSS";
	private final static DateTime now = DateTime.now();
	
	// An example composite filter string: 
	// (xpath("//a/b/c") = "foo" or xpath("//a/b/d") > 5 or xpath("//a/f") is null) and not xpath("timestamp") > time-millis("yyyy-MM-dd'T'HH:mm:ss:SSS", "2012-08-22T08:45:56:086") and xpath("//a/b/e") between (5, 10) and xpath("//a/b/f") =~ "null" and xpath("//a/g") <= time-string("yyyy-MM-dd'T'HH:mm:ss:SSS", "yyyy-MM-dd'T'HH:mm:ss:SSS", "2012-08-22T16:45:56:086") and xpath("//a/h") in (a,b,c,d) or xpath("//a/b/g/f") in (1,2,3,4) or not xpath("//no/no") exists

	@Parameters
	public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
        	{
	        	//         (xpath("//a/b/c") = "foo" or xpath("//a/b/d") > 5 or xpath("//a/f") is null) 
				// and not xpath("timestamp") > time-millis(TIME_FORMAT, 3 hours ago) 
				// and     xpath("//a/b/e") between (5, 10) 
				// and     xpath("//a/b/f") =~ "[0-9a-zA-Z]+"
				// and     xpath("//a/g") <= time-string(TIME_FORMAT, TIME_FORMAT, 5 hours later)
				// and     xpath("//a/h") in ("a", "b", "c", "d")
				//  or     xpath("//a/b/g/f") in (1, 2, 3, 4)
				//  or not xpath("//no/no") exists
				and(
					bracket(
						or(
							stringComp.create("//a/b/c", "=", "foo"), 
							numberComp.create("//a/b/d", ">", 5),
							isNull.create("//a/f")
						)
					),
	
					not(
						timeComp.create("timestamp", ">", timeMillisNHoursAway(-3, TIME_FORMAT))
					), 
					
					and(
						between.create("//a/b/e", null, new Object[]{5, 10}),
						regex.create("//a/b/f", "[0-9a-zA-Z]+"),
						timeComp.create("//a/g", "<=", timeStringNHoursAway(5, TIME_FORMAT))
					), 
					
					or(
						inPred.create("//a/h", null, wrap(new Object[]{"a", "b", "c", "d"})),
						inPred.create("//a/b/g/f", null, new Object[]{1, 2, 3, 4}),
						not (
							existsRight.create("//no/no")
						)
					)
				), 
				
				new Object[][]{
					// This event matches the filter above
					new Object[]{
						createEvent(
							new Object[]{"//a/b/c", "bar"}, // It's OK. There's a number of "or" clauses. 
							new Object[]{"//a/b/d", 10},
							new Object[]{"//a/f", "not null"}, // It's OK. The previous one is true
							new Object[]{"timestamp", nHoursAway(-4)}, // note this is not >
							new Object[]{"//a/b/e", 7},
							new Object[]{"//a/b/f", "123a43bsdfA"},
							new Object[]{"//a/g", nHoursAway(0, TIME_FORMAT)},
							new Object[]{"//a/h", "c"},
							new Object[]{"//a/b/g/f", 3},
							new Object[]{"//no/no", "something"} // this will fail the last clause
						),
						true,
					},
					
					new Object[]{
						createEvent(
							new Object[]{"//a/b/c", "bar"}, // not match 
							new Object[]{"//a/b/d", 0}, // not match
							new Object[]{"//a/f", "not null"}, // not match
							new Object[]{"timestamp", nHoursAway(-4)}, // match
							new Object[]{"//a/b/e", 7},
							new Object[]{"//a/b/f", "123a43bsdfA"},
							new Object[]{"//a/g", nHoursAway(0, TIME_FORMAT)},
							new Object[]{"//a/h", "c"},
							new Object[]{"//a/b/g/f", 10}, // match
							new Object[]{"//no/no", "something"} // match
						), 
						false
					}
				}
        	}
        });
	}
	
	private static String wrap(String value){
		return String.format("\"%s\"", value);
	}
	 
	private static String[] wrap(Object[] strings) {
		String[] result = new String[strings.length];
		
		for(int i = 0; i < strings.length; ++i) {
			result[i] = wrap(strings[i].toString());
		}
		
		return result;
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
	
	private String filterString;
	private MessageFilter filter;
	private Object[][] eventResultPairs;
	public CompositeMessageFilterParsingTest(String filterString, Object[][]eventResultPairs)
            throws InvalidFilterException {
		this.filterString = filterString;
        this.filter = MessageFilterCompiler.compile(this.filterString);
		this.eventResultPairs = eventResultPairs;
	}
	
	@Test
	public void trans() {
		String input = and(
				stringComp.create("//a/b/c", "=", "foo"), 
				numberComp.create("//a/b/d", ">", 5),
				
				between.create("//a/b/e", null, new Object[]{5, 10}),
				regex.create("//a/b/f", "[0-9a-zA-Z]+"),
				timeComp.create("//a/g", "<=", timeStringNHoursAway(5, TIME_FORMAT)),
			
				inPred.create("//a/h", null, wrap(new Object[]{"a", "b", "c", "d"}))
		);
		
		System.out.println("Generated filter string: "+input);
        MessageFilter ef = null;
        try {
            ef = MessageFilterCompiler.compile(input);
        } catch (InvalidFilterException e) {
            throw new AssertionError("Invalid filter string generated. Error: " + e.getMessage());
        }
	}
	
	@Test
	public void test() throws Exception { 
		for(Object[] pair : eventResultPairs){
			Object event = pair[0];
			boolean result = (Boolean) pair[1];
			
			assertEquals(String.format("Event object: %s\nFilter string: %s\n", event, this.filterString), result,
                         filter.apply(event));
		}
	}
	
	private static String or(String firstTerm, String secondTerm, Object...rest) {
		return Joiner.on(" or ").join(firstTerm, secondTerm, rest);
	}
	
	private static String and(String first, String second, Object...rest) {
		return Joiner.on(" and ").join(first, second, rest);
	}
	
	private static String not(String term) {
		return "not "+term;
	}
	
	private static String bracket(String term) {
		return String.format("(%s)", term);
	}
	
	private static Object createEvent(Object[]... pairs) {
		Map<String, ? super Object> object = Maps.newHashMap();
		
		for(Object[] pair: pairs) {
			String path = (String)pair[0];
			List<String> steps = toSteps(path);
			Object value = pair[1];
			
			updateObjectWithPathAndValue(object, steps, value);
		}
		
		return object;
	}
	
	private static List<String> toSteps(String xpath) {
	    List<String> steps = ImmutableList
				.copyOf(Splitter.on('/')
				.trimResults()
				.omitEmptyStrings()
				.split(xpath));
	    return steps;
    }
	// Bypass JXPathContext#createPathAndValue() because it doesn't support context-dependent predicates
	private static void updateObjectWithPathAndValue(Map<String, ? super Object> object, List<String> path, Object value) {
		if(path.isEmpty()) {
			throw new IllegalArgumentException("There should be at least one step in the given path");
		}
		
		if(path.size() == 1) {
			object.put(path.get(0), value);
			return;
		}
		
		String key = path.get(0);
		@SuppressWarnings("unchecked")
        Map<String, ? super Object> next = (Map)object.get(key);
		if(next == null){
			next =  Maps.newHashMap();
			object.put(key,next);
		}
		
		updateObjectWithPathAndValue(next, path.subList(1, path.size()), value);
		
	}
}
