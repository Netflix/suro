package com.netflix.suro.routing.filter;


/**
 * This predicate returns true if its input is null. That is, it identifies
 * null object. 
 *
 */
final public class NullValuePredicate implements ValuePredicate{

    private static final byte KEY = 0x00;
	
	private NullValuePredicate(){}
	
	@Override
    public boolean apply(final Object input) {
	    return input == null;
    }
	
	public static final NullValuePredicate INSTANCE = new NullValuePredicate();

	@Override
    public String toString() {
	    StringBuilder builder = new StringBuilder();
	    builder.append("NullValuePredicate []");
	    return builder.toString();
    }

	@Override
    public final int hashCode() {
	    return KEY;
    }

	@Override
    public final boolean equals(Object obj) {
	   return obj instanceof NullValuePredicate;
    }
	
	
}
