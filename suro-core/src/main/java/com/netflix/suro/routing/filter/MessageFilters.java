package com.netflix.suro.routing.filter;

import com.google.common.collect.ImmutableList;

/**
 * A number of static helper methods to simplify the construction of combined event filters. 
 */
public class MessageFilters {

    private MessageFilters(){}
	
    public static MessageFilter alwaysTrue(){
		return AlwaysTrueMessageFilter.INSTANCE;
	}
	
    public static MessageFilter alwaysFalse() {
		return AlwaysFalseMessageFilter.INSTANCE;
	}
	
	public static MessageFilter or(MessageFilter...filters) {
		return new OrMessageFilter(filters);
	}
	
	public static MessageFilter or(Iterable<MessageFilter> filters) {
		return new OrMessageFilter(ImmutableList.copyOf(filters));
	}
	
	public static MessageFilter and(MessageFilter...filters) {
		return new AndMessageFilter(filters);
	}
	
	public static MessageFilter and(Iterable<MessageFilter> filters){
		return new AndMessageFilter(ImmutableList.copyOf(filters));
	}
	
	public static MessageFilter not(MessageFilter filter) {
		return new NotMessageFilter(filter);
	}
}
