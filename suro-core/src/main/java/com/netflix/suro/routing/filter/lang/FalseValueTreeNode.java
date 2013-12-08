package com.netflix.suro.routing.filter.lang;

import com.netflix.suro.routing.filter.MessageFilters;
import com.netflix.suro.routing.filter.MessageFilter;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

public class FalseValueTreeNode extends MessageFilterBaseTreeNode implements ValueTreeNode, MessageFilterTranslatable {

	@Override
	public Object getValue() {
		return Boolean.FALSE;
	}

	public FalseValueTreeNode(Token t) {
		super(t);
	} 

	public FalseValueTreeNode(FalseValueTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new FalseValueTreeNode(this);
	}

	@Override
    public MessageFilter translate() {
	    return MessageFilters.alwaysFalse();
    } 
}
