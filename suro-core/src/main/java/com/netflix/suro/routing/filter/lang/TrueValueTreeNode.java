package com.netflix.suro.routing.filter.lang;

import com.netflix.suro.routing.filter.MessageFilters;
import com.netflix.suro.routing.filter.MessageFilter;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

public class TrueValueTreeNode extends MessageFilterBaseTreeNode implements MessageFilterTranslatable {

	public TrueValueTreeNode(Token t) {
		super(t);
	} 

	public TrueValueTreeNode(TrueValueTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new TrueValueTreeNode(this);
	}

	@Override
    public MessageFilter translate() {
	   return MessageFilters.alwaysTrue();
    } 
}
