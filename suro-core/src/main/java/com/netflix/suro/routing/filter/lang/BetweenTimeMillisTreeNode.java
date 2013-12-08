package com.netflix.suro.routing.filter.lang;

import com.netflix.suro.routing.filter.*;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;


public class BetweenTimeMillisTreeNode extends MessageFilterBaseTreeNode implements MessageFilterTranslatable {

	@Override
	public MessageFilter translate() {
		ValueTreeNode xpathNode = (ValueTreeNode)getChild(0);
		String xpath = (String)xpathNode.getValue(); 
		
		TimeMillisValueTreeNode lowerBoundNode = (TimeMillisValueTreeNode)getChild(1);
		
		TimeMillisValueTreeNode upperBoundNode = (TimeMillisValueTreeNode)getChild(2);
		
		return MessageFilters.and(
            new PathValueMessageFilter(
                xpath,
                new TimeMillisValuePredicate(lowerBoundNode.getValueFormat(), lowerBoundNode.getValue(), ">=")),
            new PathValueMessageFilter(
                xpath,
                new TimeMillisValuePredicate(upperBoundNode.getValueFormat(), upperBoundNode.getValue(), "<"))
        );
		
	}

	public BetweenTimeMillisTreeNode(Token t) {
		super(t);
	} 

	public BetweenTimeMillisTreeNode(BetweenTimeMillisTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new BetweenTimeMillisTreeNode(this);
	} 
}
