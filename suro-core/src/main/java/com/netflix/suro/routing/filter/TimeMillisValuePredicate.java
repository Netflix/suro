package com.netflix.suro.routing.filter;

import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nullable;

public class TimeMillisValuePredicate implements ValuePredicate<Long> {

	private String timeFormat; 
	private String value; 
	private String fnName;
	private NumericValuePredicate longPredicate; 

	public TimeMillisValuePredicate(String timeFormat, String value, String fnName){
		this.timeFormat = timeFormat;
		this.value = value;
		this.fnName = fnName; 
		
		DateTimeFormatter formatter = TimeUtil.toDateTimeFormatter("time format", timeFormat);
		
		long timeInMs = formatter.parseMillis(value);
		this.longPredicate = new NumericValuePredicate(timeInMs, fnName);
	}

	@Override
    public boolean apply(@Nullable Long input) {
	    return longPredicate.apply(input);
    }

	public String getValue(){
		return value;
	}
	
	public String getTimeFormat(){
		return this.timeFormat;
	}
	
	String getFnName() {
		return this.fnName;
	}

	@Override
    public String toString() {
	    StringBuilder builder = new StringBuilder();
	    builder.append("TimeMillisValuePredicate [timeFormat=");
	    builder.append(timeFormat);
	    builder.append(", value=");
	    builder.append(value);
	    builder.append(", fnName=");
	    builder.append(fnName);
	    builder.append(", longPredicate=");
	    builder.append(longPredicate);
	    builder.append("]");
	    return builder.toString();
    }

	@Override
    public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((fnName == null) ? 0 : fnName.hashCode());
	    result = prime * result + ((timeFormat == null) ? 0 : timeFormat.hashCode());
	    result = prime * result + ((value == null) ? 0 : value.hashCode());
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
	    TimeMillisValuePredicate other = (TimeMillisValuePredicate) obj;
	    if (fnName == null) {
		    if (other.fnName != null) {
			    return false;
		    }
	    } else if (!fnName.equals(other.fnName)) {
		    return false;
	    }
	    if (timeFormat == null) {
		    if (other.timeFormat != null) {
			    return false;
		    }
	    } else if (!timeFormat.equals(other.timeFormat)) {
		    return false;
	    }
	    if (value == null) {
		    if (other.value != null) {
			    return false;
		    }
	    } else if (!value.equals(other.value)) {
		    return false;
	    }
	    return true;
    }
}
