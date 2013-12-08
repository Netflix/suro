package com.netflix.suro.routing.filter.lang;

import com.netflix.suro.routing.filter.*;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

import static com.netflix.suro.routing.filter.lang.MessageFilterParser.*;
import static com.netflix.suro.routing.filter.lang.TreeNodeUtil.getXPath;

public abstract class EqualityComparisonBaseTreeNode extends MessageFilterBaseTreeNode {

	public EqualityComparisonBaseTreeNode(Token t) {
		super(t);
	}

	public EqualityComparisonBaseTreeNode(MessageFilterBaseTreeNode node) {
		super(node);
	}

	// TODO this is an ugly workaround. We should really generate ^(NOT ^(Equals...) for NOT_EQUAL
	// but I can't get ANTLR to generated nested tree with added node.
	protected MessageFilter getEqualFilter() {
        String xpath = getXPath(getChild(0)); 
    	
    	Tree valueNode = getChild(1);
    	
    	switch(valueNode.getType()){
    	case NUMBER:
    		Number value = (Number)((ValueTreeNode)valueNode).getValue();
    		return new PathValueMessageFilter(xpath, new NumericValuePredicate(value, "="));
    	case STRING:
    		String sValue = (String)((ValueTreeNode)valueNode).getValue();
    		return new PathValueMessageFilter(xpath, new StringValuePredicate(sValue));
    	case TRUE:
    		return new PathValueMessageFilter(xpath, BooleanValuePredicate.TRUE);
    	case FALSE:
    		return new PathValueMessageFilter(xpath, BooleanValuePredicate.FALSE);
    	case NULL:
    		return new PathValueMessageFilter(xpath, NullValuePredicate.INSTANCE);
    	case XPATH_FUN_NAME:
    		String aPath = (String)((ValueTreeNode)valueNode).getValue();
    		return new PathValueMessageFilter(xpath, new PathValuePredicate(aPath, xpath));
    	case TIME_MILLIS_FUN_NAME:
    		TimeMillisValueTreeNode timeNode = (TimeMillisValueTreeNode)valueNode;
    		return new PathValueMessageFilter(xpath,
    			new TimeMillisValuePredicate(
    				timeNode.getValueFormat(), 
    				timeNode.getValue(), 
    				"="));
    	case TIME_STRING_FUN_NAME:
    		TimeStringValueTreeNode timeStringNode = (TimeStringValueTreeNode)valueNode;
    		return new PathValueMessageFilter(xpath,
    			new TimeStringValuePredicate(
    				timeStringNode.getValueTimeFormat(), 
    				timeStringNode.getInputTimeFormat(),
    				timeStringNode.getValue(),
    				"="));
    	default:
    		throw new UnexpectedTokenException(valueNode, "Number", "String", "TRUE", "FALSE");
    	}
    }

}
