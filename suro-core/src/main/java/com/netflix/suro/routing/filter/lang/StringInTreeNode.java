package com.netflix.suro.routing.filter.lang;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.netflix.suro.routing.filter.MessageFilters;
import com.netflix.suro.routing.filter.MessageFilter;
import com.netflix.suro.routing.filter.PathValueMessageFilter;
import com.netflix.suro.routing.filter.StringValuePredicate;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

import java.util.List;

import static com.netflix.suro.routing.filter.lang.TreeNodeUtil.getXPath;


public class StringInTreeNode extends MessageFilterBaseTreeNode implements MessageFilterTranslatable {

	@SuppressWarnings("unchecked")
    @Override
	public MessageFilter translate() {
		final String xpath = getXPath(getChild(0));
		
		List children = getChildren();
		return MessageFilters.or(
            Lists.transform(children.subList(1, children.size()), new Function<Object, MessageFilter>() {

                @Override
                public MessageFilter apply(Object node) {
                    String value = ((StringTreeNode) node).getValue();
                    return new PathValueMessageFilter(xpath, new StringValuePredicate(value));
                }

            })
        );
	}

	public StringInTreeNode(Token t) {
		super(t);
	} 

	public StringInTreeNode(StringInTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new StringInTreeNode(this);
	} 
}
