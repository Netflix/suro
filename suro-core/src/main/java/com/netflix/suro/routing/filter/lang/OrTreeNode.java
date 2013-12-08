package com.netflix.suro.routing.filter.lang;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.netflix.suro.routing.filter.MessageFilters;
import com.netflix.suro.routing.filter.MessageFilter;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;

public class OrTreeNode extends CommonTree implements MessageFilterTranslatable {

	@Override
	@SuppressWarnings("unchecked")
	public MessageFilter translate() {
		return MessageFilters.or(
            Lists.transform(getChildren(), new Function<Object, MessageFilter>() {

                @Override
                public MessageFilter apply(Object input) {
                    MessageFilterTranslatable node = (MessageFilterTranslatable) input;

                    return node.translate();
                }
            })
        );
	}

	public OrTreeNode(Token t) {
		super(t);
	} 

	public OrTreeNode(OrTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new OrTreeNode(this);
	} // for dup'ing type

	public String toString() {
		return String.format("%s<%s>", token.getText(), getClass().getSimpleName());
	}
}