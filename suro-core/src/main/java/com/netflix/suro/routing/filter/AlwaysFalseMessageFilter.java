package com.netflix.suro.routing.filter;

final public class AlwaysFalseMessageFilter extends BaseMessageFilter {

	// There's no point of creating multiple instance of this class
	private AlwaysFalseMessageFilter(){
        setOriginalDslString("false");
    }
	
	@Override
    public boolean apply(Object input) {
	    return false;
    }

	public static final AlwaysFalseMessageFilter INSTANCE = new AlwaysFalseMessageFilter();

    @Override
    public String toString() {
	    StringBuilder builder = new StringBuilder();
	    builder.append("AlwaysFalseMessageFilter []");
	    return builder.toString();
    }

	@Override
	public int hashCode() {
		return Boolean.FALSE.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
        return obj instanceof AlwaysFalseMessageFilter;
    }
}
