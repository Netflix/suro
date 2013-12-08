package com.netflix.suro.routing.filter.lang;

import com.netflix.suro.routing.filter.PathExistsMessageFilter;
import com.netflix.suro.routing.filter.MessageFilter;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

import static com.netflix.suro.routing.filter.lang.TreeNodeUtil.getXPath;

public class ExistsTreeNode extends MessageFilterBaseTreeNode implements MessageFilterTranslatable {

	@Override
	public MessageFilter translate() {
		return new PathExistsMessageFilter(getXPath(getChild(0)));
	}

	public ExistsTreeNode(Token t) {
		super(t);
	} 

	public ExistsTreeNode(ExistsTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new ExistsTreeNode(this);
	} 
}
