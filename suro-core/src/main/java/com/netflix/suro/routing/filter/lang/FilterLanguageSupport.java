package com.netflix.suro.routing.filter.lang;

import com.netflix.suro.routing.filter.MessageFilter;

/**
 * General contract for any filter language which relates to a methodology of converting a language expression to a
 * valid {@link com.netflix.suro.routing.filter.MessageFilter} instance consumable by {@link com.netflix.suro.routing.filter}
 *
 * @author Nitesh Kant (nkant@netflix.com)
 */
public interface FilterLanguageSupport<T> {

    /**
     * Converts the passed filter object to a valid {@link MessageFilter}.
     *
     * @param filter Filter object to convert.
     *
     * @return {@link MessageFilter} corresponding to the passed filter.
     *
     * @throws InvalidFilterException If the passed filter was invalid.
     */
    MessageFilter convert(T filter) throws InvalidFilterException;
}
