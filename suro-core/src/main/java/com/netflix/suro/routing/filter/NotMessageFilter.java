package com.netflix.suro.routing.filter;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

public class NotMessageFilter extends BaseMessageFilter {

	final private Predicate<Object> notPredicate;

	public NotMessageFilter(MessageFilter filter) {

        this.notPredicate = Predicates.not(filter);
	}

    @Override
    public boolean apply(Object input) {
        return notPredicate.apply(input);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("NotMessageFilter");
        sb.append("{notPredicate=").append(notPredicate);
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

        NotMessageFilter that = (NotMessageFilter) o;

        if (notPredicate != null ? !notPredicate.equals(that.notPredicate) : that.notPredicate != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return notPredicate != null ? notPredicate.hashCode() : 0;
    }
}
