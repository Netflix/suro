package com.netflix.suro.routing.filter.lang;


import com.netflix.suro.routing.filter.MessageFilters;
import com.netflix.suro.routing.filter.MessageFilter;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

public class NotEqualsTreeNode extends EqualityComparisonBaseTreeNode implements MessageFilterTranslatable {

	@Override
	public MessageFilter translate() {
		return MessageFilters.not(getEqualFilter());
    }

    public NotEqualsTreeNode(Token t) {
		super(t);
	} 

	public NotEqualsTreeNode(NotEqualsTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new NotEqualsTreeNode(this);
	} 
}
