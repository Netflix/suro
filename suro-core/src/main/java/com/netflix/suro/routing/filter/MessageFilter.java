package com.netflix.suro.routing.filter;

import com.google.common.base.Predicate;

/**
 * A contract for filtering events. These filters can be applied/defined both at the publisher and subscriber level.<p/>
 * It is recommended to use a filter language as specified in {@link com.netflix.suro.routing.filter.lang} which provides
 * flexible ways of defining filters. However, for programmatic creation of simple or custom filters it may be easy
 * to directly implement this interface. <p/>
 * The structure of the event filters is entirely opaque to the event bus and all processing related to evaluation of
 * the same is left to the implementations.
 */
public interface MessageFilter extends Predicate<Object> {

    // Emphasize that every {@code MessageFilter} instance can be used as a key
    // in a collection.
    public int hashCode();

    public boolean equals(Object o);
}
