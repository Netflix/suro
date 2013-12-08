package com.netflix.suro.routing.filter;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.regex.Pattern;

public class RegexValuePredicate implements ValuePredicate<String> {

	private String regex; 
	private Pattern pattern; 
	private MatchPolicy policy;
	
	public RegexValuePredicate(String regex, MatchPolicy policy){
		Preconditions.checkArgument(regex != null, String.format("The regex should not be null."));
		this.regex = regex;
		this.pattern = Pattern.compile(regex);
		this.policy = policy;
	}
	
	@Override
    public boolean apply(@Nullable String input) {
		if(input == null){
			return false;
		}
		
	    if(policy == MatchPolicy.PARTIAL){
	    	return pattern.matcher(input).find();
	    }
	    
	    if(policy == MatchPolicy.FULL){
	    	return pattern.matcher(input).matches();
	    }
	    
	    throw new UnsupportedOperationException(String.format("the match policy %s is not supported", policy));
    }
	
	public String getPattern() {
		return this.pattern.pattern();
	}
	
	public MatchPolicy getMatchPolicy() {
		return policy;
	}

	@Override
    public String toString() {
	    StringBuilder builder = new StringBuilder();
	    builder.append("RegexValuePredicate [pattern=");
	    builder.append(regex);
	    builder.append(", policy=");
	    builder.append(policy);
	    builder.append("]");
	    return builder.toString();
    }

	public static enum MatchPolicy {
		PARTIAL, 
		FULL
	}

	@Override
    public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((policy == null) ? 0 : policy.hashCode());
	    result = prime * result + ((regex == null) ? 0 : regex.hashCode());
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
	    RegexValuePredicate other = (RegexValuePredicate) obj;
	    if (policy != other.policy) {
		    return false;
	    }
	    if (regex == null) {
		    if (other.regex != null) {
			    return false;
		    }
	    } else if (!regex.equals(other.regex)) {
		    return false;
	    }
	    return true;
    }
}
