package com.netflix.suro.routing.filter.lang;

import com.netflix.suro.routing.filter.MessageFilters;
import com.netflix.suro.routing.filter.MessageFilter;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

public class NotTreeNode extends MessageFilterBaseTreeNode implements MessageFilterTranslatable {

	@Override
	public MessageFilter translate() {
		MessageFilter filter = ((MessageFilterTranslatable)getChild(0)).translate();
		
		return MessageFilters.not(filter);
	}

	public NotTreeNode(Token t) {
		super(t);
	} 

	public NotTreeNode(NotTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new NotTreeNode(this);
	} 
}
