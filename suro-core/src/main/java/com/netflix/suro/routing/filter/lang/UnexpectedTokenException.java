package com.netflix.suro.routing.filter.lang;

import com.google.common.base.Joiner;
import org.antlr.runtime.tree.Tree;

public class UnexpectedTokenException extends RuntimeException {
	private Tree unexpected;
	private String[] expected;
	private Joiner joiner = Joiner.on(" or ");
	public UnexpectedTokenException(Tree unexpected, String... expected){
		this.unexpected = unexpected;
		this.expected = expected;
	}
	
	@Override
	public String toString() {
		return String.format(
			"Unexpected token %s at %d:%d. Expected: %s", 
			unexpected.getText(), 
			unexpected.getLine(), 
			unexpected.getCharPositionInLine(), 
			joiner.join(expected));
	}
	
	@Override
	public String getMessage(){
		return toString();
	}
}
