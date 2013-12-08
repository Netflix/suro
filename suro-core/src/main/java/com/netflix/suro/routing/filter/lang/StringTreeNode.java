package com.netflix.suro.routing.filter.lang;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

public class StringTreeNode extends MessageFilterBaseTreeNode implements ValueTreeNode {

	@Override
	public String getValue() {
		return getText();
	}

	public StringTreeNode(Token t) {
		super(t);
	} 

	public StringTreeNode(StringTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new StringTreeNode(this);
	} 
}
