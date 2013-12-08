package com.netflix.suro.routing.filter.lang;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

public class NullValueTreeNode extends MessageFilterBaseTreeNode implements ValueTreeNode {

	@Override
	public Object getValue() {
		return null;
		
	}

	public NullValueTreeNode(Token t) {
		super(t);
	} 

	public NullValueTreeNode(NullValueTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new NullValueTreeNode(this);
	} 
}
