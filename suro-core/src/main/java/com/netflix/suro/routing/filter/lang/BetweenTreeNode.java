package com.netflix.suro.routing.filter.lang;


import com.netflix.suro.routing.filter.*;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

public class BetweenTreeNode extends MessageFilterBaseTreeNode implements MessageFilterTranslatable {

	@Override
	public MessageFilter translate() {
		ValueTreeNode xpathNode = (ValueTreeNode)getChild(0);
		String xpath = (String)xpathNode.getValue(); 
		
		ValueTreeNode lowerBoundNode = (ValueTreeNode)getChild(1);
		Number lowerBound = (Number)lowerBoundNode.getValue();
		
		ValueTreeNode upperBoundNode = (ValueTreeNode)getChild(2);
		Number upperBound = (Number)upperBoundNode.getValue(); 
		
		return MessageFilters.and(
            new PathValueMessageFilter(xpath, new NumericValuePredicate(lowerBound, ">=")),
            new PathValueMessageFilter(xpath, new NumericValuePredicate(upperBound, "<"))
        );
		
	}

	public BetweenTreeNode(Token t) {
		super(t);
	} 

	public BetweenTreeNode(BetweenTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new BetweenTreeNode(this);
	} 
}
