package com.netflix.suro.routing.filter.lang;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.netflix.suro.routing.filter.MessageFilter;
import com.netflix.suro.routing.filter.MessageFilters;
import com.netflix.suro.routing.filter.NumericValuePredicate;
import com.netflix.suro.routing.filter.PathValueMessageFilter;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;

import java.util.List;

import static com.netflix.suro.routing.filter.lang.TreeNodeUtil.getXPath;

public class NumericInTreeNode extends MessageFilterBaseTreeNode implements MessageFilterTranslatable {

	@SuppressWarnings("unchecked")
    @Override
	public MessageFilter translate() {
		final String xpath = getXPath(getChild(0));
		
		List children = getChildren();
		return MessageFilters.or(
                Lists.transform(children.subList(1, children.size()), new Function<Object, MessageFilter>() {

                    @Override
                    public MessageFilter apply(Object node) {
                        Number value = ((NumberTreeNode) node).getValue();
                        return new PathValueMessageFilter(xpath, new NumericValuePredicate(value, "="));
                    }

                })
        );
	}

	public NumericInTreeNode(Token t) {
		super(t);
	} 

	public NumericInTreeNode(NumericInTreeNode node) {
		super(node);
	} 

	public Tree dupNode() {
		return new NumericInTreeNode(this);
	} 
}
