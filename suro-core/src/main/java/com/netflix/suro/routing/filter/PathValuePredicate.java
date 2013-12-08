package com.netflix.suro.routing.filter;

import com.google.common.base.Objects;
import org.apache.commons.jxpath.JXPathContext;

import javax.annotation.Nullable;

public class PathValuePredicate implements ValuePredicate<String> {

	private String valueXpath;
	private String inputXpath; 
	
	public PathValuePredicate(String valueXpath, String inputXpath){
		this.valueXpath = valueXpath;
		this.inputXpath = inputXpath;
	}
	
	@Override
    public boolean apply(@Nullable String input) {
		JXPathContext context = JXPathContext.newContext(input);
		context.setLenient(true);
	    return Objects.equal(context.getValue(valueXpath), context.getValue(inputXpath));
    }

	public String getInputXpath(){
		return inputXpath;
	}

	public String getValueXpath() {
		return valueXpath;
	}

	@Override
    public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((inputXpath == null) ? 0 : inputXpath.hashCode());
	    result = prime * result + ((valueXpath == null) ? 0 : valueXpath.hashCode());
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
	    PathValuePredicate other = (PathValuePredicate) obj;
	    if (inputXpath == null) {
		    if (other.inputXpath != null) {
			    return false;
		    }
	    } else if (!inputXpath.equals(other.inputXpath)) {
		    return false;
	    }
	    if (valueXpath == null) {
		    if (other.valueXpath != null) {
			    return false;
		    }
	    } else if (!valueXpath.equals(other.valueXpath)) {
		    return false;
	    }
	    return true;
    }

	@Override
    public String toString() {
	    StringBuilder builder = new StringBuilder();
	    builder.append("PathValuePredicate [valueXpath=");
	    builder.append(valueXpath);
	    builder.append(", inputXpath=");
	    builder.append(inputXpath);
	    builder.append("]");
	    return builder.toString();
    }

}
