package com.netflix.suro.routing.filter;

import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.jxpath.Pointer;
import org.apache.commons.jxpath.ri.model.beans.NullPointer;

public class PathExistsMessageFilter extends BaseMessageFilter {

	private String xpath;

	public PathExistsMessageFilter(String path) {
		this.xpath = path;
    }
	
	
    @Override
    public boolean apply(Object input) {
        JXPathContext jxpath = JXPathContext.newContext(input);
        // We should allow non-existing path, and let predicate handle it.
        jxpath.setLenient(true);

        Pointer pointer = jxpath.getPointer(xpath);
       
        return pointer != null && !(pointer instanceof NullPointer) && pointer.getValue() != null;
    }
	
	public String getXpath() {
    	return xpath;
    }

    @Override
    public String toString() {
	    StringBuilder builder = new StringBuilder();
	    builder.append("PathExistsMessageFilter [xpath=");
	    builder.append(xpath);
	    builder.append("]");
	    return builder.toString();
    }


	@Override
    public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((xpath == null) ? 0 : xpath.hashCode());
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
	    PathExistsMessageFilter other = (PathExistsMessageFilter) obj;
	    if (xpath == null) {
		    if (other.xpath != null) {
			    return false;
		    }
	    } else if (!xpath.equals(other.xpath)) {
		    return false;
	    }
	    return true;
    }

	
}
