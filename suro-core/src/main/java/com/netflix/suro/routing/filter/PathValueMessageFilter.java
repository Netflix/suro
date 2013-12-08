package com.netflix.suro.routing.filter;

import org.apache.commons.jxpath.JXPathContext;

public class PathValueMessageFilter extends BaseMessageFilter {

	private String xpath; 
	private ValuePredicate predicate;

	public PathValueMessageFilter(String path, ValuePredicate predicate) {
		this.xpath = path;
		this.predicate = predicate;
    }
	
	
    @SuppressWarnings("unchecked")
    @Override
    public boolean apply(Object input) {
        JXPathContext jxpath = JXPathContext.newContext(input);
        // We should allow non-existing path, and let predicate handle it. 
        jxpath.setLenient(true);
        
        Object value = jxpath.getValue(xpath);
       
        return predicate.apply(value);
    }
	
	public String getXpath() {
    	return xpath;
    }

	public ValuePredicate getPredicate() {
    	return predicate;
    }

    @Override
    public String toString() {
	    StringBuilder builder = new StringBuilder();
	    builder.append("PathValueMessageFilter [xpath=");
	    builder.append(xpath);
	    builder.append(", predicate=");
	    builder.append(predicate);
	    builder.append("]");
	    return builder.toString();
    }


	@Override
    public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((predicate == null) ? 0 : predicate.hashCode());
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
	    PathValueMessageFilter other = (PathValueMessageFilter) obj;
	    if (predicate == null) {
		    if (other.predicate != null) {
			    return false;
		    }
	    } else if (!predicate.equals(other.predicate)) {
		    return false;
	    }
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
