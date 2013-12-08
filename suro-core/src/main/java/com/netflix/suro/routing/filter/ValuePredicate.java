package com.netflix.suro.routing.filter;

import com.google.common.base.Predicate;

public interface ValuePredicate<T> extends Predicate<T>{

	// Emphasize that every {@code ValuePredicate} instance can be used as a key
	// in a collection.
	public int hashCode(); 
	public boolean equals(Object o);
}
