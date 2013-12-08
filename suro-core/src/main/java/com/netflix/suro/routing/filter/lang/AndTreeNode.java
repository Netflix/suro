package com.netflix.suro.routing.filter.lang;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.netflix.suro.routing.filter.MessageFilters;
import com.netflix.suro.routing.filter.MessageFilter;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;


public class AndTreeNode extends MessageFilterBaseTreeNode implements MessageFilterTranslatable {

	@Override
	@SuppressWarnings("unchecked")
	public MessageFilter translate() {
		return MessageFilters.and(
            Lists.transform(getChildren(), new Function<Object, MessageFilter>() {

                @Override
                public MessageFilter apply(Object input) {
                    MessageFilterTranslatable node = (MessageFilterTranslatable) input;

                    return node.translate();
                }
            })
        );
	}

	public AndTreeNode(Token t) {
		super(t);
	} 

	public AndTreeNode(AndTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new AndTreeNode(this);
	} 
}
