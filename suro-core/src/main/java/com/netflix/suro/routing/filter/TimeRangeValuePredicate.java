package com.netflix.suro.routing.filter;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nullable;

public class TimeRangeValuePredicate implements ValuePredicate<Long> {

	private String timeFormat; 
	private String start; 
	private String end;
	private Interval interval; 

	public TimeRangeValuePredicate(String timeFormat, String start, String end){
		
		Preconditions.checkArgument(!Strings.isNullOrEmpty(timeFormat), "Time format can't be null or empty. ");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(start), "The lower bound of time range can't be null or empty.");
		Preconditions.checkArgument(!Strings.isNullOrEmpty(end), "The upper bound of time range  can't be null or empty. ");
		
		this.timeFormat = timeFormat;
		this.start = start;
		this.end = end; 
		
		DateTimeFormatter format = TimeUtil.toDateTimeFormatter("time format", timeFormat);
		long startInstant = format.parseMillis(start);
		long endInstant = format.parseMillis(end);
		
		try{
			this.interval = new Interval(startInstant, endInstant);
		}catch(IllegalArgumentException e) {
			String msg = String.format(
				"The lower bound should be smaller than or equal to upper bound for a time range. Given range: [%s, %s)",
				start,
				end);
			IllegalArgumentException iae = new IllegalArgumentException(msg, e.getCause());
			iae.setStackTrace(e.getStackTrace());
			throw iae;
		}
	}
	
	@Override
    public boolean apply(@Nullable Long input) {
	    return null != input && this.interval.contains(input);
    }

	public String getStart(){
		return start;
	}
	
	public String getTimeFormat(){
		return this.timeFormat;
	}
	
	String getEnd() {
		return this.end;
	}

	@Override
    public String toString() {
	    StringBuilder builder = new StringBuilder();
	    builder.append("TimeRangeValuePredicate [timeFormat=");
	    builder.append(timeFormat);
	    builder.append(", start=");
	    builder.append(start);
	    builder.append(", end=");
	    builder.append(end);
	    builder.append(", interval=");
	    builder.append(interval);
	    builder.append("]");
	    return builder.toString();
    }

	@Override
    public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((end == null) ? 0 : end.hashCode());
	    result = prime * result + ((start == null) ? 0 : start.hashCode());
	    result = prime * result + ((timeFormat == null) ? 0 : timeFormat.hashCode());
	    return result;
    }

	@Override
    public boolean equals(Object obj) {
	    if (this == obj) {
		    return true;
	    }
	    if (obj == null) {
		    return false;
	    }
	    if (getClass() != obj.getClass()) {
		    return false;
	    }
	    TimeRangeValuePredicate other = (TimeRangeValuePredicate) obj;
	    if (end == null) {
		    if (other.end != null) {
			    return false;
		    }
	    } else if (!end.equals(other.end)) {
		    return false;
	    }
	    if (start == null) {
		    if (other.start != null) {
			    return false;
		    }
	    } else if (!start.equals(other.start)) {
		    return false;
	    }
	    if (timeFormat == null) {
		    if (other.timeFormat != null) {
			    return false;
		    }
	    } else if (!timeFormat.equals(other.timeFormat)) {
		    return false;
	    }
	    return true;
    }
}
