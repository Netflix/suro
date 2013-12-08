package com.netflix.suro.routing.filter.lang;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

public class XPathTreeNode extends MessageFilterBaseTreeNode implements ValueTreeNode {

	@Override
	public Object getValue() {
		return getChild(0).getText();
	}

	public XPathTreeNode(Token t) {
		super(t);
	} 

	public XPathTreeNode(XPathTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new XPathTreeNode(this);
	} 
}
