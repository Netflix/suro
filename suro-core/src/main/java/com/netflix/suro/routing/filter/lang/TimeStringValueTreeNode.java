package com.netflix.suro.routing.filter.lang;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

public class TimeStringValueTreeNode extends MessageFilterBaseTreeNode implements ValueTreeNode {

	@Override
	public String getValue() {
		return (String)((ValueTreeNode)getChild(2)).getValue(); 
	}

	public String getValueTimeFormat() {
		return (String)((ValueTreeNode)getChild(1)).getValue();
	}
	
	public String getInputTimeFormat() {
		return (String)((ValueTreeNode)getChild(0)).getValue();
	}
	
	public TimeStringValueTreeNode(Token t) {
		super(t);
	} 

	public TimeStringValueTreeNode(TimeStringValueTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new TimeStringValueTreeNode(this);
	} 
}
