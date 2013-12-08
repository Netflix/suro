package com.netflix.suro.routing.filter;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

public class OrMessageFilter extends BaseMessageFilter {

	final private Predicate<Object> orPredicate;

	public OrMessageFilter(MessageFilter... filters) {

        this.orPredicate = Predicates.or(filters);
	}

    public OrMessageFilter(Iterable<? extends MessageFilter> filters) {

        this.orPredicate = Predicates.or(filters);
    }

    @Override
    public boolean apply(Object input) {
        return orPredicate.apply(input);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("OrMessageFilter");
        sb.append("{orPredicate=").append(orPredicate);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OrMessageFilter that = (OrMessageFilter) o;

        if (orPredicate != null ? !orPredicate.equals(that.orPredicate) : that.orPredicate != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return orPredicate != null ? orPredicate.hashCode() : 0;
    }
}
