package com.netflix.suro.routing.filter;

import com.google.common.base.Objects;

import javax.annotation.Nullable;

public class StringValuePredicate implements ValuePredicate<String> {
	private String value;
	
	public StringValuePredicate(@Nullable String value){
		this.value = value;
	}
	
	@Override
    public boolean apply(@Nullable String input) {
	    return Objects.equal(value, input);
    }

	String getValue(){
		return value;
	}

	@Override
    public String toString() {
	    StringBuilder builder = new StringBuilder();
	    builder.append("StringValuePredicate [value=");
	    builder.append(value);
	    builder.append("]");
	    return builder.toString();
    }

	@Override
    public int hashCode() {
	    final int prime = 31;
	    int result = 1;
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
	    StringValuePredicate other = (StringValuePredicate) obj;
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
