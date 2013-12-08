package com.netflix.suro.routing.filter.lang;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;

public abstract class MessageFilterBaseTreeNode extends CommonTree {
	public MessageFilterBaseTreeNode(Token t) {
		super(t);
	} 

	public MessageFilterBaseTreeNode(MessageFilterBaseTreeNode node) {
	    super(node);
    }

	public String toString() {
		return String.format("%s<%s>", getText(), getClass().getSimpleName());
	}
}
