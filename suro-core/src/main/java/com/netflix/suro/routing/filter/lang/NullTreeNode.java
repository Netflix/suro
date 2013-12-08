package com.netflix.suro.routing.filter.lang;

import com.netflix.suro.routing.filter.NullValuePredicate;
import com.netflix.suro.routing.filter.PathValueMessageFilter;
import com.netflix.suro.routing.filter.MessageFilter;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

import static com.netflix.suro.routing.filter.lang.TreeNodeUtil.getXPath;

public class NullTreeNode extends MessageFilterBaseTreeNode implements MessageFilterTranslatable {

	@Override
	public MessageFilter translate() {
		String xpath = getXPath(getChild(0));
		
		return new PathValueMessageFilter(xpath, NullValuePredicate.INSTANCE);
	}

	public NullTreeNode(Token t) {
		super(t);
	} 

	public NullTreeNode(NullTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new NullTreeNode(this);
	} 
}
