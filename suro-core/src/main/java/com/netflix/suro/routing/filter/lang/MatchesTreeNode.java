package com.netflix.suro.routing.filter.lang;

import com.netflix.suro.routing.filter.PathValueMessageFilter;
import com.netflix.suro.routing.filter.RegexValuePredicate;
import com.netflix.suro.routing.filter.MessageFilter;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

import static com.netflix.suro.routing.filter.lang.TreeNodeUtil.getXPath;

public class MatchesTreeNode extends MessageFilterBaseTreeNode implements MessageFilterTranslatable {

	@Override
	public MessageFilter translate() {
		String xpath = getXPath(getChild(0)); 
    	
		StringTreeNode valueNode = (StringTreeNode)getChild(1);

        return new PathValueMessageFilter(xpath, new RegexValuePredicate(valueNode.getValue(), RegexValuePredicate.MatchPolicy.PARTIAL));

    }

	public MatchesTreeNode(Token t) {
		super(t);
	} 

	public MatchesTreeNode(MatchesTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new MatchesTreeNode(this);
	} 
}
