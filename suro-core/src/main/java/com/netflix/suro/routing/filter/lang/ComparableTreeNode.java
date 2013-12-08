package com.netflix.suro.routing.filter.lang;

import com.netflix.suro.routing.filter.*;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

import static com.netflix.suro.routing.filter.lang.TreeNodeUtil.getXPath;
import static com.netflix.suro.routing.filter.lang.MessageFilterParser.*;

public class ComparableTreeNode extends MessageFilterBaseTreeNode implements MessageFilterTranslatable {

	@Override
	public MessageFilter translate() {
		String xpath = getXPath(getChild(0)); 
		
		Tree valueNode = getChild(1);
		
		switch(valueNode.getType()){
		case NUMBER:
			Number value = (Number)((ValueTreeNode)valueNode).getValue();
			return new PathValueMessageFilter(xpath, new NumericValuePredicate(value, getToken().getText()));
		case TIME_MILLIS_FUN_NAME:
			TimeMillisValueTreeNode timeValueNode = (TimeMillisValueTreeNode)valueNode;
			return new PathValueMessageFilter(
				xpath, 
				new TimeMillisValuePredicate(
					timeValueNode.getValueFormat(), 
					timeValueNode.getValue(), 
					getToken().getText()));
		case TIME_STRING_FUN_NAME:
			TimeStringValueTreeNode timeStringNode = (TimeStringValueTreeNode)valueNode;
			
			return new PathValueMessageFilter(
				xpath, 
				new TimeStringValuePredicate(
					timeStringNode.getValueTimeFormat(), 
					timeStringNode.getInputTimeFormat(), 
					timeStringNode.getValue(), 
					getToken().getText()));
		default:
			throw new UnexpectedTokenException(valueNode, "Number", "time-millis", "time-string");
		}
		
	}

	
	public ComparableTreeNode(Token t) {
		super(t);
	} 

	public ComparableTreeNode(ComparableTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new ComparableTreeNode(this);
	} 
}
