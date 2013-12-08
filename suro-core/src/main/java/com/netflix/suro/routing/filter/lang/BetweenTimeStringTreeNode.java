package com.netflix.suro.routing.filter.lang;

import com.netflix.suro.routing.filter.*;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

public class BetweenTimeStringTreeNode extends MessageFilterBaseTreeNode implements MessageFilterTranslatable {

	@Override
	public MessageFilter translate() {
		ValueTreeNode xpathNode = (ValueTreeNode)getChild(0);
		String xpath = (String)xpathNode.getValue(); 
		
		TimeStringValueTreeNode lowerBoundNode = (TimeStringValueTreeNode)getChild(1);
		
		TimeStringValueTreeNode upperBoundNode = (TimeStringValueTreeNode)getChild(2);
		
		return MessageFilters.and(
            new PathValueMessageFilter(
                xpath,
                new TimeStringValuePredicate(
                    lowerBoundNode.getValueTimeFormat(),
                    lowerBoundNode.getInputTimeFormat(),
                    lowerBoundNode.getValue(),
                    ">=")),
            new PathValueMessageFilter(
                xpath,
                new TimeStringValuePredicate(
                    upperBoundNode.getValueTimeFormat(),
                    upperBoundNode.getInputTimeFormat(),
                    upperBoundNode.getValue(),
                    "<"))
        );
		
	}

	public BetweenTimeStringTreeNode(Token t) {
		super(t);
	} 

	public BetweenTimeStringTreeNode(BetweenTimeStringTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new BetweenTimeStringTreeNode(this);
	} 
}
