grammar MessageFilter;
options {
	language = Java;
	output = AST;
	ASTLabelType = CommonTree;
}

tokens{
	SELECT = 				'select';
	WHERE = 				'where';
	EQUALS =				'='; 
	NOT_EQUALS =			'!=';
	OR	=					'or';
	AND	=  					'and';
	NOT	= 					'not';
	GT =					'>';
	GE =  					'>=';
	LT =					'<';
	LE = 					'<=';
	MATCHES =				'=~';
	
	BETWEEN =				'between';
	IN =  					'in';
	IS =					'is';
	EXISTS =				'exists';
		
	TRUE =					'true';
	FALSE =  				'false';
	NULL =  				'null';
	
	XPATH_FUN_NAME = 		'xpath';
	TIME_MILLIS_FUN_NAME = 	'time-millis';
	TIME_STRING_FUN_NAME =  'time-string';
}

// Do not use indentation for package declarations, or generated code will 
// indent package declarations too
@header {
package com.netflix.suro.routing.filter.lang;
}

@lexer::header {
package com.netflix.suro.routing.filter.lang;
}

@lexer::members {
	public void reportError(RecognitionException e) {
		// if we've already reported an error and have not matched a token
		// yet successfully, don't report any errors.
		if ( state.errorRecovery ) {
			//System.err.print("[SPURIOUS] ");
			return;
		}
		
		state.syntaxErrors++; // don't count spurious
		state.errorRecovery = true;

		throwLexerException(this.getTokenNames(), e);
	}

	public void throwLexerException(String[] tokenNames, RecognitionException e) {
		String hdr = getErrorHeader(e);
		String msg = getErrorMessage(e, tokenNames);
		
		throw new MessageFilterParsingException(hdr+" "+msg, e);
	}
}

@members {
	/**
		Creates a new parser that parses the given input string.
	*/
	public static MessageFilterParser createParser(String input) {
	    ANTLRStringStream inputStream = new ANTLRStringStream(input);
		MessageFilterLexer lexer = new MessageFilterLexer(inputStream);
		CommonTokenStream tokens = new CommonTokenStream(lexer); 
		
		return new MessageFilterParser(tokens);
    }
	
	@Override
	public void reportError(RecognitionException e) {
		// if we've already reported an error and have not matched a token
		// yet successfully, don't report any errors.
		if ( state.errorRecovery ) {
			return;
		}
		
		state.syntaxErrors++; // don't count spurious
		state.errorRecovery = true;

		throwParsingError(this.getTokenNames(), e);
	}
	
	// A slight modification of #displayRecognitionError(String[], RecognitionException)
	private void throwParsingError(String[] tokenNames, RecognitionException e) {
		String hdr = getErrorHeader(e);
		String msg = getErrorMessage(e, tokenNames);
		
		throw new MessageFilterParsingException(String.format("\%s \%s", hdr, msg), e);
	}
}

filter
	:	(a=boolean_expr->$a) (OR b=boolean_expr -> ^(OR<OrTreeNode> $filter $b) )* EOF?
	;
	
boolean_expr
	:	(a=boolean_factor->$a) (AND b=boolean_factor -> ^(AND<AndTreeNode> $boolean_expr $b) )* EOF?
		
	;

boolean_factor
	:	predicate |
	    NOT predicate -> ^(NOT<NotTreeNode> predicate) 
	;	

predicate
	:	'(' filter ')' -> filter | 
		comparison_function |
		between_predicate |
		in_predicate |
		null_predicate |
		regex_predicate |
		exists_predicate |
		TRUE -> TRUE<TrueValueTreeNode>|
		FALSE -> FALSE<FalseValueTreeNode> 
	;

comparison_function
	:	path_function EQUALS value_function -> ^(EQUALS<EqualsTreeNode> path_function value_function) |
		path_function NOT_EQUALS value_function -> ^(NOT_EQUALS<NotEqualsTreeNode> path_function value_function) |
		path_function GT compariable_value_function -> ^(GT<ComparableTreeNode> path_function compariable_value_function) |
		path_function GE compariable_value_function -> ^(GE<ComparableTreeNode> path_function compariable_value_function) |
		path_function LT compariable_value_function -> ^(LT<ComparableTreeNode> path_function compariable_value_function) |
		path_function LE compariable_value_function -> ^(LE<ComparableTreeNode> path_function compariable_value_function) 
	;

between_predicate
	:	path_function BETWEEN '(' NUMBER ',' NUMBER ')' 
			-> ^(BETWEEN<BetweenTreeNode> path_function NUMBER<NumberTreeNode> NUMBER<NumberTreeNode>) |
		path_function BETWEEN '(' time_millis_function ',' time_millis_function ')' 
			-> ^(BETWEEN<BetweenTimeMillisTreeNode> path_function time_millis_function time_millis_function) |
		path_function BETWEEN '(' time_string_function ',' time_string_function ')' 
			-> ^(BETWEEN<BetweenTimeStringTreeNode> path_function time_string_function time_string_function) 
	;

in_predicate
	:	path_function IN '(' STRING (',' STRING)* ')' -> ^(IN<StringInTreeNode> path_function (STRING<StringTreeNode>)+) |
		path_function IN '(' NUMBER (',' NUMBER)* ')' -> ^(IN<NumericInTreeNode> path_function (NUMBER<NumberTreeNode>)+)
	;

null_predicate
	:	path_function IS NULL -> ^(NULL<NullTreeNode> path_function)
	;

regex_predicate
	:   path_function MATCHES STRING -> ^(MATCHES<MatchesTreeNode> path_function STRING<StringTreeNode>)
	;

exists_predicate
	:	path_function EXISTS -> ^(EXISTS<ExistsTreeNode> path_function) |
		EXISTS path_function -> ^(EXISTS<ExistsTreeNode> path_function)
	;
							
path_function
	: XPATH_FUN_NAME '(' STRING ')' -> ^(XPATH_FUN_NAME<XPathTreeNode> STRING<StringTreeNode>)	
	;

value_function
	:	equality_value_function | compariable_value_function
	;
	
equality_value_function
	:	STRING -> STRING<StringTreeNode> |
		TRUE -> TRUE<TrueValueTreeNode> |
		FALSE -> FALSE<FalseValueTreeNode> |
		NULL -> NULL<NullValueTreeNode> |
		path_function
	;
	
compariable_value_function
	:	NUMBER -> NUMBER<NumberTreeNode>|
		time_millis_function |
		time_string_function
	;
	
time_millis_function
	:	TIME_MILLIS_FUN_NAME '(' STRING ',' STRING ')' -> ^(TIME_MILLIS_FUN_NAME<TimeMillisValueTreeNode>  STRING<StringTreeNode> STRING<StringTreeNode>) |
	;

/**
time-string(inputTimeFormat, valueTimeFormat, value) 
*/	
time_string_function
	: 	TIME_STRING_FUN_NAME '(' STRING ',' STRING ',' STRING ')' -> ^(TIME_STRING_FUN_NAME<TimeStringValueTreeNode> STRING<StringTreeNode> STRING<StringTreeNode> STRING<StringTreeNode>)
	;
	
NUMBER
    :   ('+'|'-')? ('0'..'9')+ ('.' ('0'..'9')* EXPONENT?)?
    |   ('+'|'-')? '.' ('0'..'9')+ EXPONENT?
    |   ('+'|'-')? ('0'..'9')+ EXPONENT
    ;
	
COMMENT
    :   '//' ~('\n'|'\r')* '\r'? '\n' {$channel=HIDDEN;}
    |   '/*' ( options {greedy=false;} : . )* '*/' {$channel=HIDDEN;}
    ;
    	
WS  :   ( ' '
        | '\t'
        | '\r'
        | '\n'
        ) {$channel=HIDDEN;}
    ;
	
STRING
    :  '"' ( ESC_SEQ | ~('\\'|'"') )* '"' {setText(getText().substring(1, getText().length()-1));} // Stripping surrounding quotes
    ;

fragment
HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;

fragment
ESC_SEQ
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UNICODE_ESC
    |   OCTAL_ESC
    ;

fragment
OCTAL_ESC
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

fragment
UNICODE_ESC
    :   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;

fragment
EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;
