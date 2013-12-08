package com.netflix.suro.routing.filter;

import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nullable;

public class TimeStringValuePredicate implements ValuePredicate<String> {

	private String valueFormat;
	private String inputFormat;
	private String value; 
	private String fnName;
	private DateTimeFormatter inputTimeFormatter;
	private TimeMillisValuePredicate timePredicate; 
	
	public TimeStringValuePredicate(String valueFormat, String inputFormat, String value, String fnName){
		this.valueFormat = valueFormat;
		this.inputFormat = inputFormat;
		this.value = value;
		this.fnName = fnName; 
		
		this.inputTimeFormatter  = TimeUtil.toDateTimeFormatter("input format", inputFormat);
		this.timePredicate = new TimeMillisValuePredicate(this.valueFormat, value, fnName);
	}
	
	@Override
    public boolean apply(@Nullable String input) {
		long timeValue = inputTimeFormatter.parseMillis(input);
	    return timePredicate.apply(timeValue);
    }

	public String getValue(){
		return value;
	}
	
	public String getValueFormat(){
		return this.valueFormat;
	}
	
	public String getInputFormat() {
		return this.inputFormat;
	}
	
	String getFnName() {
		return this.fnName;
	}

	@Override
    public String toString() {
	    StringBuilder builder = new StringBuilder();
	    builder.append("TimeStringValuePredicate [valueFormat=");
	    builder.append(valueFormat);
	    builder.append(", inputFormat=");
	    builder.append(inputFormat);
	    builder.append(", value=");
	    builder.append(value);
	    builder.append(", fnName=");
	    builder.append(fnName);
	    builder.append(", inputTimeFormatter=");
	    builder.append(inputTimeFormatter);
	    builder.append(", timePredicate=");
	    builder.append(timePredicate);
	    builder.append("]");
	    return builder.toString();
    }

	@Override
    public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((fnName == null) ? 0 : fnName.hashCode());
	    result = prime * result + ((inputFormat == null) ? 0 : inputFormat.hashCode());
	    result = prime * result + ((value == null) ? 0 : value.hashCode());
	    result = prime * result + ((valueFormat == null) ? 0 : valueFormat.hashCode());
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
	    TimeStringValuePredicate other = (TimeStringValuePredicate) obj;
	    if (fnName == null) {
		    if (other.fnName != null) {
			    return false;
		    }
	    } else if (!fnName.equals(other.fnName)) {
		    return false;
	    }
	    if (inputFormat == null) {
		    if (other.inputFormat != null) {
			    return false;
		    }
	    } else if (!inputFormat.equals(other.inputFormat)) {
		    return false;
	    }
	    if (value == null) {
		    if (other.value != null) {
			    return false;
		    }
	    } else if (!value.equals(other.value)) {
		    return false;
	    }
	    if (valueFormat == null) {
		    if (other.valueFormat != null) {
			    return false;
		    }
	    } else if (!valueFormat.equals(other.valueFormat)) {
		    return false;
	    }
	    return true;
    }
}
