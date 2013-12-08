package com.netflix.suro.routing.filter;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class TimeUtil {

	/**
	 * Converts the given time format string to a {@link org.joda.time.format.DateTimeFormatter} instance.
	 *
	 * @param formatName A name used to identify the given time format. This is mainly used for error reporting.
	 * @param timeFormat The date time format to be converted.
	 *
	 * @return A {@link org.joda.time.format.DateTimeFormatter} instance of the given time format.
	 *
	 * @throws IllegalArgumentException if the given time format is invalid.
	 */
	public static DateTimeFormatter toDateTimeFormatter(String formatName, String timeFormat) {
        DateTimeFormatter formatter = null;
    	try{
    		formatter = DateTimeFormat.forPattern(timeFormat);
    	}catch(IllegalArgumentException e){
    		//JODA's error message doesn't tell you which value sucked, so we create a better
    		// error message here.
    		IllegalArgumentException iae = new IllegalArgumentException(
    			String.format("Invalid time format for the property %s: '%s'",
    				formatName,
    				timeFormat),
    			e.getCause());
    		iae.setStackTrace(e.getStackTrace());

    		throw iae;
    	}
        return formatter;
    }

	/**
	 * Converts the given epoch time in millisecond to a string according to the given format. Note
	 * each invocation creates a new {@link org.joda.time.format.DateTimeFormatter} instance, which is pretty costly.
	 * This method is suitable for testing and calls that are not on hot path. 
	 * 
	 * @param millis the epoch time to be converted
	 * @param format The format the returned time string
	 * @return A string representation of the given epoch time in the given format
	 */
	public static String toString(long millis, String format){
		return DateTimeFormat.forPattern(format).print(millis);
	}
}
