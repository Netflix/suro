package com.netflix.suro.routing.filter.lang;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

public class NumberTreeNode extends MessageFilterBaseTreeNode implements ValueTreeNode {

	@Override
	public Number getValue() {
		return Double.valueOf(getText());
	}

	public NumberTreeNode(Token t) {
		super(t);
	} 

	public NumberTreeNode(NumberTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new NumberTreeNode(this);
	} 
}
