package com.netflix.suro.routing.filter;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

public class AndMessageFilter extends BaseMessageFilter {

	final private Predicate<Object> andPredicate;

    public AndMessageFilter(MessageFilter... filters) {

        this.andPredicate = Predicates.and(filters);
	}

	public AndMessageFilter(Iterable<? extends MessageFilter> filters) {

        this.andPredicate = Predicates.and(filters);
	}

    @Override
    public boolean apply(Object input) {
        return andPredicate.apply(input);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AndMessageFilter that = (AndMessageFilter) o;

        if (andPredicate != null ? !andPredicate.equals(that.andPredicate) : that.andPredicate != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return andPredicate != null ? andPredicate.hashCode() : 0;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("AndMessageFilter");
        sb.append("{andPredicate=").append(andPredicate);
        sb.append('}');
        return sb.toString();
    }
}
