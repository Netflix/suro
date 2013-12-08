package com.netflix.suro.routing.filter;

final public class AlwaysTrueMessageFilter extends BaseMessageFilter {

    private AlwaysTrueMessageFilter() {
        setOriginalDslString("true");
	}

	@Override
	public boolean apply(Object input) {
		return true;
	}

	public static final AlwaysTrueMessageFilter INSTANCE = new AlwaysTrueMessageFilter();

    @Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("AlwaysTrueMessageFilter []");
		return builder.toString();
	}

	@Override
	public int hashCode() {
		return Boolean.TRUE.hashCode();
	}

	@Override
	public boolean equals(Object obj){
		return obj instanceof AlwaysTrueMessageFilter;
	}
}
