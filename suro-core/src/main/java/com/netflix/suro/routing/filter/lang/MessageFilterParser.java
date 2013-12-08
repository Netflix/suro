// $ANTLR 3.4 MessageFilter.g 2012-08-22 11:55:58

package com.netflix.suro.routing.filter.lang;


import org.antlr.runtime.*;
import org.antlr.runtime.tree.*;


@SuppressWarnings({"all", "warnings", "unchecked"})
public class MessageFilterParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "AND", "BETWEEN", "COMMENT", "EQUALS", "ESC_SEQ", "EXISTS", "EXPONENT", "FALSE", "GE", "GT", "HEX_DIGIT", "IN", "IS", "LE", "LT", "MATCHES", "NOT", "NOT_EQUALS", "NULL", "NUMBER", "OCTAL_ESC", "OR", "STRING", "TIME_MILLIS_FUN_NAME", "TIME_STRING_FUN_NAME", "TRUE", "UNICODE_ESC", "WS", "XPATH_FUN_NAME", "'('", "')'", "','"
    };

    public static final int EOF=-1;
    public static final int T__33=33;
    public static final int T__34=34;
    public static final int T__35=35;
    public static final int AND=4;
    public static final int BETWEEN=5;
    public static final int COMMENT=6;
    public static final int EQUALS=7;
    public static final int ESC_SEQ=8;
    public static final int EXISTS=9;
    public static final int EXPONENT=10;
    public static final int FALSE=11;
    public static final int GE=12;
    public static final int GT=13;
    public static final int HEX_DIGIT=14;
    public static final int IN=15;
    public static final int IS=16;
    public static final int LE=17;
    public static final int LT=18;
    public static final int MATCHES=19;
    public static final int NOT=20;
    public static final int NOT_EQUALS=21;
    public static final int NULL=22;
    public static final int NUMBER=23;
    public static final int OCTAL_ESC=24;
    public static final int OR=25;
    public static final int STRING=26;
    public static final int TIME_MILLIS_FUN_NAME=27;
    public static final int TIME_STRING_FUN_NAME=28;
    public static final int TRUE=29;
    public static final int UNICODE_ESC=30;
    public static final int WS=31;
    public static final int XPATH_FUN_NAME=32;

    // delegates
    public Parser[] getDelegates() {
        return new Parser[] {};
    }

    // delegators


    public MessageFilterParser(TokenStream input) {
        this(input, new RecognizerSharedState());
    }
    public MessageFilterParser(TokenStream input, RecognizerSharedState state) {
        super(input, state);
    }

protected TreeAdaptor adaptor = new CommonTreeAdaptor();

public void setTreeAdaptor(TreeAdaptor adaptor) {
    this.adaptor = adaptor;
}
public TreeAdaptor getTreeAdaptor() {
    return adaptor;
}
    public String[] getTokenNames() { return MessageFilterParser.tokenNames; }
    public String getGrammarFileName() { return "MessageFilter.g"; }


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
    		
    		throw new MessageFilterParsingException(String.format("%s %s", hdr, msg), e);
    	}


    public static class filter_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "filter"
    // MessageFilter.g:102:1: filter : (a= boolean_expr -> $a) ( OR b= boolean_expr -> ^( OR $filter $b) )* ( EOF )? ;
    public final filter_return filter() throws RecognitionException {
        filter_return retval = new filter_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token OR1=null;
        Token EOF2=null;
        boolean_expr_return a =null;

        boolean_expr_return b =null;


        CommonTree OR1_tree=null;
        CommonTree EOF2_tree=null;
        RewriteRuleTokenStream stream_EOF=new RewriteRuleTokenStream(adaptor,"token EOF");
        RewriteRuleTokenStream stream_OR=new RewriteRuleTokenStream(adaptor,"token OR");
        RewriteRuleSubtreeStream stream_boolean_expr=new RewriteRuleSubtreeStream(adaptor,"rule boolean_expr");
        try {
            // MessageFilter.g:103:2: ( (a= boolean_expr -> $a) ( OR b= boolean_expr -> ^( OR $filter $b) )* ( EOF )? )
            // MessageFilter.g:103:4: (a= boolean_expr -> $a) ( OR b= boolean_expr -> ^( OR $filter $b) )* ( EOF )?
            {
            // MessageFilter.g:103:4: (a= boolean_expr -> $a)
            // MessageFilter.g:103:5: a= boolean_expr
            {
            pushFollow(FOLLOW_boolean_expr_in_filter323);
            a=boolean_expr();

            state._fsp--;

            stream_boolean_expr.add(a.getTree());

            // AST REWRITE
            // elements: a
            // token labels:
            // rule labels: retval, a
            // token list labels:
            // rule list labels:
            // wildcard labels:
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"rule a",a!=null?a.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 103:19: -> $a
            {
                adaptor.addChild(root_0, stream_a.nextTree());

            }


            retval.tree = root_0;

            }


            // MessageFilter.g:103:25: ( OR b= boolean_expr -> ^( OR $filter $b) )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( (LA1_0==OR) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // MessageFilter.g:103:26: OR b= boolean_expr
            	    {
            	    OR1=(Token)match(input,OR,FOLLOW_OR_in_filter330);
            	    stream_OR.add(OR1);


            	    pushFollow(FOLLOW_boolean_expr_in_filter334);
            	    b=boolean_expr();

            	    state._fsp--;

            	    stream_boolean_expr.add(b.getTree());

            	    // AST REWRITE
            	    // elements: OR, filter, b
            	    // token labels:
            	    // rule labels: retval, b
            	    // token list labels:
            	    // rule list labels:
            	    // wildcard labels:
            	    retval.tree = root_0;
            	    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            	    RewriteRuleSubtreeStream stream_b=new RewriteRuleSubtreeStream(adaptor,"rule b",b!=null?b.tree:null);

            	    root_0 = (CommonTree)adaptor.nil();
            	    // 103:44: -> ^( OR $filter $b)
            	    {
            	        // MessageFilter.g:103:47: ^( OR $filter $b)
            	        {
            	        CommonTree root_1 = (CommonTree)adaptor.nil();
            	        root_1 = (CommonTree)adaptor.becomeRoot(
            	        new OrTreeNode(stream_OR.nextToken())
            	        , root_1);

            	        adaptor.addChild(root_1, stream_retval.nextTree());

            	        adaptor.addChild(root_1, stream_b.nextTree());

            	        adaptor.addChild(root_0, root_1);
            	        }

            	    }


            	    retval.tree = root_0;

            	    }
            	    break;

            	default :
            	    break loop1;
                }
            } while (true);


            // MessageFilter.g:103:79: ( EOF )?
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0==EOF) ) {
                alt2=1;
            }
            switch (alt2) {
                case 1 :
                    // MessageFilter.g:103:79: EOF
                    {
                    EOF2=(Token)match(input,EOF,FOLLOW_EOF_in_filter354);
                    stream_EOF.add(EOF2);


                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "filter"


    public static class boolean_expr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "boolean_expr"
    // MessageFilter.g:106:1: boolean_expr : (a= boolean_factor -> $a) ( AND b= boolean_factor -> ^( AND $boolean_expr $b) )* ;
    public final boolean_expr_return boolean_expr() throws RecognitionException {
        boolean_expr_return retval = new boolean_expr_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token AND3=null;
        boolean_factor_return a =null;

        boolean_factor_return b =null;


        CommonTree AND3_tree=null;
        RewriteRuleTokenStream stream_AND=new RewriteRuleTokenStream(adaptor,"token AND");
        RewriteRuleSubtreeStream stream_boolean_factor=new RewriteRuleSubtreeStream(adaptor,"rule boolean_factor");
        try {
            // MessageFilter.g:107:2: ( (a= boolean_factor -> $a) ( AND b= boolean_factor -> ^( AND $boolean_expr $b) )* )
            // MessageFilter.g:107:4: (a= boolean_factor -> $a) ( AND b= boolean_factor -> ^( AND $boolean_expr $b) )*
            {
            // MessageFilter.g:107:4: (a= boolean_factor -> $a)
            // MessageFilter.g:107:5: a= boolean_factor
            {
            pushFollow(FOLLOW_boolean_factor_in_boolean_expr370);
            a=boolean_factor();

            state._fsp--;

            stream_boolean_factor.add(a.getTree());

            // AST REWRITE
            // elements: a
            // token labels:
            // rule labels: retval, a
            // token list labels:
            // rule list labels:
            // wildcard labels:
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_a=new RewriteRuleSubtreeStream(adaptor,"rule a",a!=null?a.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 107:21: -> $a
            {
                adaptor.addChild(root_0, stream_a.nextTree());

            }


            retval.tree = root_0;

            }


            // MessageFilter.g:107:27: ( AND b= boolean_factor -> ^( AND $boolean_expr $b) )*
            loop3:
            do {
                int alt3=2;
                int LA3_0 = input.LA(1);

                if ( (LA3_0==AND) ) {
                    alt3=1;
                }


                switch (alt3) {
            	case 1 :
            	    // MessageFilter.g:107:28: AND b= boolean_factor
            	    {
            	    AND3=(Token)match(input,AND,FOLLOW_AND_in_boolean_expr377);
            	    stream_AND.add(AND3);


            	    pushFollow(FOLLOW_boolean_factor_in_boolean_expr381);
            	    b=boolean_factor();

            	    state._fsp--;

            	    stream_boolean_factor.add(b.getTree());

            	    // AST REWRITE
            	    // elements: b, boolean_expr, AND
            	    // token labels:
            	    // rule labels: retval, b
            	    // token list labels:
            	    // rule list labels:
            	    // wildcard labels:
            	    retval.tree = root_0;
            	    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            	    RewriteRuleSubtreeStream stream_b=new RewriteRuleSubtreeStream(adaptor,"rule b",b!=null?b.tree:null);

            	    root_0 = (CommonTree)adaptor.nil();
            	    // 107:49: -> ^( AND $boolean_expr $b)
            	    {
            	        // MessageFilter.g:107:52: ^( AND $boolean_expr $b)
            	        {
            	        CommonTree root_1 = (CommonTree)adaptor.nil();
            	        root_1 = (CommonTree)adaptor.becomeRoot(
            	        new AndTreeNode(stream_AND.nextToken())
            	        , root_1);

            	        adaptor.addChild(root_1, stream_retval.nextTree());

            	        adaptor.addChild(root_1, stream_b.nextTree());

            	        adaptor.addChild(root_0, root_1);
            	        }

            	    }


            	    retval.tree = root_0;

            	    }
            	    break;

            	default :
            	    break loop3;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "boolean_expr"


    public static class boolean_factor_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "boolean_factor"
    // MessageFilter.g:111:1: boolean_factor : ( predicate | NOT predicate -> ^( NOT predicate ) );
    public final boolean_factor_return boolean_factor() throws RecognitionException {
        boolean_factor_return retval = new boolean_factor_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token NOT5=null;
        predicate_return predicate4 =null;

        predicate_return predicate6 =null;


        CommonTree NOT5_tree=null;
        RewriteRuleTokenStream stream_NOT=new RewriteRuleTokenStream(adaptor,"token NOT");
        RewriteRuleSubtreeStream stream_predicate=new RewriteRuleSubtreeStream(adaptor,"rule predicate");
        try {
            // MessageFilter.g:112:2: ( predicate | NOT predicate -> ^( NOT predicate ) )
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==EXISTS||LA4_0==FALSE||LA4_0==TRUE||(LA4_0 >= XPATH_FUN_NAME && LA4_0 <= 33)) ) {
                alt4=1;
            }
            else if ( (LA4_0==NOT) ) {
                alt4=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 4, 0, input);

                throw nvae;

            }
            switch (alt4) {
                case 1 :
                    // MessageFilter.g:112:4: predicate
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_predicate_in_boolean_factor414);
                    predicate4=predicate();

                    state._fsp--;

                    adaptor.addChild(root_0, predicate4.getTree());

                    }
                    break;
                case 2 :
                    // MessageFilter.g:113:6: NOT predicate
                    {
                    NOT5=(Token)match(input,NOT,FOLLOW_NOT_in_boolean_factor423);
                    stream_NOT.add(NOT5);


                    pushFollow(FOLLOW_predicate_in_boolean_factor425);
                    predicate6=predicate();

                    state._fsp--;

                    stream_predicate.add(predicate6.getTree());

                    // AST REWRITE
                    // elements: NOT, predicate
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 113:20: -> ^( NOT predicate )
                    {
                        // MessageFilter.g:113:23: ^( NOT predicate )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        new NotTreeNode(stream_NOT.nextToken())
                        , root_1);

                        adaptor.addChild(root_1, stream_predicate.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "boolean_factor"


    public static class predicate_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "predicate"
    // MessageFilter.g:116:1: predicate : ( '(' filter ')' -> filter | comparison_function | between_predicate | in_predicate | null_predicate | regex_predicate | exists_predicate | TRUE -> TRUE | FALSE -> FALSE );
    public final predicate_return predicate() throws RecognitionException {
        predicate_return retval = new predicate_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token char_literal7=null;
        Token char_literal9=null;
        Token TRUE16=null;
        Token FALSE17=null;
        filter_return filter8 =null;

        comparison_function_return comparison_function10 =null;

        between_predicate_return between_predicate11 =null;

        in_predicate_return in_predicate12 =null;

        null_predicate_return null_predicate13 =null;

        regex_predicate_return regex_predicate14 =null;

        exists_predicate_return exists_predicate15 =null;


        CommonTree char_literal7_tree=null;
        CommonTree char_literal9_tree=null;
        CommonTree TRUE16_tree=null;
        CommonTree FALSE17_tree=null;
        RewriteRuleTokenStream stream_FALSE=new RewriteRuleTokenStream(adaptor,"token FALSE");
        RewriteRuleTokenStream stream_TRUE=new RewriteRuleTokenStream(adaptor,"token TRUE");
        RewriteRuleTokenStream stream_33=new RewriteRuleTokenStream(adaptor,"token 33");
        RewriteRuleTokenStream stream_34=new RewriteRuleTokenStream(adaptor,"token 34");
        RewriteRuleSubtreeStream stream_filter=new RewriteRuleSubtreeStream(adaptor,"rule filter");
        try {
            // MessageFilter.g:117:2: ( '(' filter ')' -> filter | comparison_function | between_predicate | in_predicate | null_predicate | regex_predicate | exists_predicate | TRUE -> TRUE | FALSE -> FALSE )
            int alt5=9;
            switch ( input.LA(1) ) {
            case 33:
                {
                alt5=1;
                }
                break;
            case XPATH_FUN_NAME:
                {
                int LA5_2 = input.LA(2);

                if ( (LA5_2==33) ) {
                    int LA5_6 = input.LA(3);

                    if ( (LA5_6==STRING) ) {
                        int LA5_7 = input.LA(4);

                        if ( (LA5_7==34) ) {
                            switch ( input.LA(5) ) {
                            case EQUALS:
                            case GE:
                            case GT:
                            case LE:
                            case LT:
                            case NOT_EQUALS:
                                {
                                alt5=2;
                                }
                                break;
                            case BETWEEN:
                                {
                                alt5=3;
                                }
                                break;
                            case IN:
                                {
                                alt5=4;
                                }
                                break;
                            case IS:
                                {
                                alt5=5;
                                }
                                break;
                            case MATCHES:
                                {
                                alt5=6;
                                }
                                break;
                            case EXISTS:
                                {
                                alt5=7;
                                }
                                break;
                            default:
                                NoViableAltException nvae =
                                    new NoViableAltException("", 5, 8, input);

                                throw nvae;

                            }

                        }
                        else {
                            NoViableAltException nvae =
                                new NoViableAltException("", 5, 7, input);

                            throw nvae;

                        }
                    }
                    else {
                        NoViableAltException nvae =
                            new NoViableAltException("", 5, 6, input);

                        throw nvae;

                    }
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 5, 2, input);

                    throw nvae;

                }
                }
                break;
            case EXISTS:
                {
                alt5=7;
                }
                break;
            case TRUE:
                {
                alt5=8;
                }
                break;
            case FALSE:
                {
                alt5=9;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 5, 0, input);

                throw nvae;

            }

            switch (alt5) {
                case 1 :
                    // MessageFilter.g:117:4: '(' filter ')'
                    {
                    char_literal7=(Token)match(input,33,FOLLOW_33_in_predicate448);
                    stream_33.add(char_literal7);


                    pushFollow(FOLLOW_filter_in_predicate450);
                    filter8=filter();

                    state._fsp--;

                    stream_filter.add(filter8.getTree());

                    char_literal9=(Token)match(input,34,FOLLOW_34_in_predicate452);
                    stream_34.add(char_literal9);


                    // AST REWRITE
                    // elements: filter
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 117:19: -> filter
                    {
                        adaptor.addChild(root_0, stream_filter.nextTree());

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 2 :
                    // MessageFilter.g:118:3: comparison_function
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_comparison_function_in_predicate463);
                    comparison_function10=comparison_function();

                    state._fsp--;

                    adaptor.addChild(root_0, comparison_function10.getTree());

                    }
                    break;
                case 3 :
                    // MessageFilter.g:119:3: between_predicate
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_between_predicate_in_predicate469);
                    between_predicate11=between_predicate();

                    state._fsp--;

                    adaptor.addChild(root_0, between_predicate11.getTree());

                    }
                    break;
                case 4 :
                    // MessageFilter.g:120:3: in_predicate
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_in_predicate_in_predicate475);
                    in_predicate12=in_predicate();

                    state._fsp--;

                    adaptor.addChild(root_0, in_predicate12.getTree());

                    }
                    break;
                case 5 :
                    // MessageFilter.g:121:3: null_predicate
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_null_predicate_in_predicate481);
                    null_predicate13=null_predicate();

                    state._fsp--;

                    adaptor.addChild(root_0, null_predicate13.getTree());

                    }
                    break;
                case 6 :
                    // MessageFilter.g:122:3: regex_predicate
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_regex_predicate_in_predicate487);
                    regex_predicate14=regex_predicate();

                    state._fsp--;

                    adaptor.addChild(root_0, regex_predicate14.getTree());

                    }
                    break;
                case 7 :
                    // MessageFilter.g:123:3: exists_predicate
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_exists_predicate_in_predicate493);
                    exists_predicate15=exists_predicate();

                    state._fsp--;

                    adaptor.addChild(root_0, exists_predicate15.getTree());

                    }
                    break;
                case 8 :
                    // MessageFilter.g:124:3: TRUE
                    {
                    TRUE16=(Token)match(input,TRUE,FOLLOW_TRUE_in_predicate499);
                    stream_TRUE.add(TRUE16);


                    // AST REWRITE
                    // elements: TRUE
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 124:8: -> TRUE
                    {
                        adaptor.addChild(root_0,
                        new TrueValueTreeNode(stream_TRUE.nextToken())
                        );

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 9 :
                    // MessageFilter.g:125:3: FALSE
                    {
                    FALSE17=(Token)match(input,FALSE,FOLLOW_FALSE_in_predicate511);
                    stream_FALSE.add(FALSE17);


                    // AST REWRITE
                    // elements: FALSE
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 125:9: -> FALSE
                    {
                        adaptor.addChild(root_0,
                        new FalseValueTreeNode(stream_FALSE.nextToken())
                        );

                    }


                    retval.tree = root_0;

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "predicate"


    public static class comparison_function_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "comparison_function"
    // MessageFilter.g:128:1: comparison_function : ( path_function EQUALS value_function -> ^( EQUALS path_function value_function ) | path_function NOT_EQUALS value_function -> ^( NOT_EQUALS path_function value_function ) | path_function GT compariable_value_function -> ^( GT path_function compariable_value_function ) | path_function GE compariable_value_function -> ^( GE path_function compariable_value_function ) | path_function LT compariable_value_function -> ^( LT path_function compariable_value_function ) | path_function LE compariable_value_function -> ^( LE path_function compariable_value_function ) );
    public final comparison_function_return comparison_function() throws RecognitionException {
        comparison_function_return retval = new comparison_function_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token EQUALS19=null;
        Token NOT_EQUALS22=null;
        Token GT25=null;
        Token GE28=null;
        Token LT31=null;
        Token LE34=null;
        path_function_return path_function18 =null;

        value_function_return value_function20 =null;

        path_function_return path_function21 =null;

        value_function_return value_function23 =null;

        path_function_return path_function24 =null;

        compariable_value_function_return compariable_value_function26 =null;

        path_function_return path_function27 =null;

        compariable_value_function_return compariable_value_function29 =null;

        path_function_return path_function30 =null;

        compariable_value_function_return compariable_value_function32 =null;

        path_function_return path_function33 =null;

        compariable_value_function_return compariable_value_function35 =null;


        CommonTree EQUALS19_tree=null;
        CommonTree NOT_EQUALS22_tree=null;
        CommonTree GT25_tree=null;
        CommonTree GE28_tree=null;
        CommonTree LT31_tree=null;
        CommonTree LE34_tree=null;
        RewriteRuleTokenStream stream_GE=new RewriteRuleTokenStream(adaptor,"token GE");
        RewriteRuleTokenStream stream_GT=new RewriteRuleTokenStream(adaptor,"token GT");
        RewriteRuleTokenStream stream_LT=new RewriteRuleTokenStream(adaptor,"token LT");
        RewriteRuleTokenStream stream_EQUALS=new RewriteRuleTokenStream(adaptor,"token EQUALS");
        RewriteRuleTokenStream stream_NOT_EQUALS=new RewriteRuleTokenStream(adaptor,"token NOT_EQUALS");
        RewriteRuleTokenStream stream_LE=new RewriteRuleTokenStream(adaptor,"token LE");
        RewriteRuleSubtreeStream stream_compariable_value_function=new RewriteRuleSubtreeStream(adaptor,"rule compariable_value_function");
        RewriteRuleSubtreeStream stream_value_function=new RewriteRuleSubtreeStream(adaptor,"rule value_function");
        RewriteRuleSubtreeStream stream_path_function=new RewriteRuleSubtreeStream(adaptor,"rule path_function");
        try {
            // MessageFilter.g:129:2: ( path_function EQUALS value_function -> ^( EQUALS path_function value_function ) | path_function NOT_EQUALS value_function -> ^( NOT_EQUALS path_function value_function ) | path_function GT compariable_value_function -> ^( GT path_function compariable_value_function ) | path_function GE compariable_value_function -> ^( GE path_function compariable_value_function ) | path_function LT compariable_value_function -> ^( LT path_function compariable_value_function ) | path_function LE compariable_value_function -> ^( LE path_function compariable_value_function ) )
            int alt6=6;
            int LA6_0 = input.LA(1);

            if ( (LA6_0==XPATH_FUN_NAME) ) {
                int LA6_1 = input.LA(2);

                if ( (LA6_1==33) ) {
                    int LA6_2 = input.LA(3);

                    if ( (LA6_2==STRING) ) {
                        int LA6_3 = input.LA(4);

                        if ( (LA6_3==34) ) {
                            switch ( input.LA(5) ) {
                            case EQUALS:
                                {
                                alt6=1;
                                }
                                break;
                            case NOT_EQUALS:
                                {
                                alt6=2;
                                }
                                break;
                            case GT:
                                {
                                alt6=3;
                                }
                                break;
                            case GE:
                                {
                                alt6=4;
                                }
                                break;
                            case LT:
                                {
                                alt6=5;
                                }
                                break;
                            case LE:
                                {
                                alt6=6;
                                }
                                break;
                            default:
                                NoViableAltException nvae =
                                    new NoViableAltException("", 6, 4, input);

                                throw nvae;

                            }

                        }
                        else {
                            NoViableAltException nvae =
                                new NoViableAltException("", 6, 3, input);

                            throw nvae;

                        }
                    }
                    else {
                        NoViableAltException nvae =
                            new NoViableAltException("", 6, 2, input);

                        throw nvae;

                    }
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 6, 1, input);

                    throw nvae;

                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 6, 0, input);

                throw nvae;

            }
            switch (alt6) {
                case 1 :
                    // MessageFilter.g:129:4: path_function EQUALS value_function
                    {
                    pushFollow(FOLLOW_path_function_in_comparison_function529);
                    path_function18=path_function();

                    state._fsp--;

                    stream_path_function.add(path_function18.getTree());

                    EQUALS19=(Token)match(input,EQUALS,FOLLOW_EQUALS_in_comparison_function531);
                    stream_EQUALS.add(EQUALS19);


                    pushFollow(FOLLOW_value_function_in_comparison_function533);
                    value_function20=value_function();

                    state._fsp--;

                    stream_value_function.add(value_function20.getTree());

                    // AST REWRITE
                    // elements: value_function, path_function, EQUALS
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 129:40: -> ^( EQUALS path_function value_function )
                    {
                        // MessageFilter.g:129:43: ^( EQUALS path_function value_function )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        new EqualsTreeNode(stream_EQUALS.nextToken())
                        , root_1);

                        adaptor.addChild(root_1, stream_path_function.nextTree());

                        adaptor.addChild(root_1, stream_value_function.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 2 :
                    // MessageFilter.g:130:3: path_function NOT_EQUALS value_function
                    {
                    pushFollow(FOLLOW_path_function_in_comparison_function552);
                    path_function21=path_function();

                    state._fsp--;

                    stream_path_function.add(path_function21.getTree());

                    NOT_EQUALS22=(Token)match(input,NOT_EQUALS,FOLLOW_NOT_EQUALS_in_comparison_function554);
                    stream_NOT_EQUALS.add(NOT_EQUALS22);


                    pushFollow(FOLLOW_value_function_in_comparison_function556);
                    value_function23=value_function();

                    state._fsp--;

                    stream_value_function.add(value_function23.getTree());

                    // AST REWRITE
                    // elements: value_function, path_function, NOT_EQUALS
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 130:43: -> ^( NOT_EQUALS path_function value_function )
                    {
                        // MessageFilter.g:130:46: ^( NOT_EQUALS path_function value_function )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        new NotEqualsTreeNode(stream_NOT_EQUALS.nextToken())
                        , root_1);

                        adaptor.addChild(root_1, stream_path_function.nextTree());

                        adaptor.addChild(root_1, stream_value_function.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 3 :
                    // MessageFilter.g:131:3: path_function GT compariable_value_function
                    {
                    pushFollow(FOLLOW_path_function_in_comparison_function575);
                    path_function24=path_function();

                    state._fsp--;

                    stream_path_function.add(path_function24.getTree());

                    GT25=(Token)match(input,GT,FOLLOW_GT_in_comparison_function577);
                    stream_GT.add(GT25);


                    pushFollow(FOLLOW_compariable_value_function_in_comparison_function579);
                    compariable_value_function26=compariable_value_function();

                    state._fsp--;

                    stream_compariable_value_function.add(compariable_value_function26.getTree());

                    // AST REWRITE
                    // elements: path_function, GT, compariable_value_function
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 131:47: -> ^( GT path_function compariable_value_function )
                    {
                        // MessageFilter.g:131:50: ^( GT path_function compariable_value_function )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        new ComparableTreeNode(stream_GT.nextToken())
                        , root_1);

                        adaptor.addChild(root_1, stream_path_function.nextTree());

                        adaptor.addChild(root_1, stream_compariable_value_function.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 4 :
                    // MessageFilter.g:132:3: path_function GE compariable_value_function
                    {
                    pushFollow(FOLLOW_path_function_in_comparison_function598);
                    path_function27=path_function();

                    state._fsp--;

                    stream_path_function.add(path_function27.getTree());

                    GE28=(Token)match(input,GE,FOLLOW_GE_in_comparison_function600);
                    stream_GE.add(GE28);


                    pushFollow(FOLLOW_compariable_value_function_in_comparison_function602);
                    compariable_value_function29=compariable_value_function();

                    state._fsp--;

                    stream_compariable_value_function.add(compariable_value_function29.getTree());

                    // AST REWRITE
                    // elements: compariable_value_function, path_function, GE
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 132:47: -> ^( GE path_function compariable_value_function )
                    {
                        // MessageFilter.g:132:50: ^( GE path_function compariable_value_function )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        new ComparableTreeNode(stream_GE.nextToken())
                        , root_1);

                        adaptor.addChild(root_1, stream_path_function.nextTree());

                        adaptor.addChild(root_1, stream_compariable_value_function.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 5 :
                    // MessageFilter.g:133:3: path_function LT compariable_value_function
                    {
                    pushFollow(FOLLOW_path_function_in_comparison_function621);
                    path_function30=path_function();

                    state._fsp--;

                    stream_path_function.add(path_function30.getTree());

                    LT31=(Token)match(input,LT,FOLLOW_LT_in_comparison_function623);
                    stream_LT.add(LT31);


                    pushFollow(FOLLOW_compariable_value_function_in_comparison_function625);
                    compariable_value_function32=compariable_value_function();

                    state._fsp--;

                    stream_compariable_value_function.add(compariable_value_function32.getTree());

                    // AST REWRITE
                    // elements: path_function, compariable_value_function, LT
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 133:47: -> ^( LT path_function compariable_value_function )
                    {
                        // MessageFilter.g:133:50: ^( LT path_function compariable_value_function )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        new ComparableTreeNode(stream_LT.nextToken())
                        , root_1);

                        adaptor.addChild(root_1, stream_path_function.nextTree());

                        adaptor.addChild(root_1, stream_compariable_value_function.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 6 :
                    // MessageFilter.g:134:3: path_function LE compariable_value_function
                    {
                    pushFollow(FOLLOW_path_function_in_comparison_function644);
                    path_function33=path_function();

                    state._fsp--;

                    stream_path_function.add(path_function33.getTree());

                    LE34=(Token)match(input,LE,FOLLOW_LE_in_comparison_function646);
                    stream_LE.add(LE34);


                    pushFollow(FOLLOW_compariable_value_function_in_comparison_function648);
                    compariable_value_function35=compariable_value_function();

                    state._fsp--;

                    stream_compariable_value_function.add(compariable_value_function35.getTree());

                    // AST REWRITE
                    // elements: compariable_value_function, LE, path_function
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 134:47: -> ^( LE path_function compariable_value_function )
                    {
                        // MessageFilter.g:134:50: ^( LE path_function compariable_value_function )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        new ComparableTreeNode(stream_LE.nextToken())
                        , root_1);

                        adaptor.addChild(root_1, stream_path_function.nextTree());

                        adaptor.addChild(root_1, stream_compariable_value_function.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "comparison_function"


    public static class between_predicate_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "between_predicate"
    // MessageFilter.g:137:1: between_predicate : ( path_function BETWEEN '(' NUMBER ',' NUMBER ')' -> ^( BETWEEN path_function NUMBER NUMBER ) | path_function BETWEEN '(' time_millis_function ',' time_millis_function ')' -> ^( BETWEEN path_function time_millis_function time_millis_function ) | path_function BETWEEN '(' time_string_function ',' time_string_function ')' -> ^( BETWEEN path_function time_string_function time_string_function ) );
    public final between_predicate_return between_predicate() throws RecognitionException {
        between_predicate_return retval = new between_predicate_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token BETWEEN37=null;
        Token char_literal38=null;
        Token NUMBER39=null;
        Token char_literal40=null;
        Token NUMBER41=null;
        Token char_literal42=null;
        Token BETWEEN44=null;
        Token char_literal45=null;
        Token char_literal47=null;
        Token char_literal49=null;
        Token BETWEEN51=null;
        Token char_literal52=null;
        Token char_literal54=null;
        Token char_literal56=null;
        path_function_return path_function36 =null;

        path_function_return path_function43 =null;

        time_millis_function_return time_millis_function46 =null;

        time_millis_function_return time_millis_function48 =null;

        path_function_return path_function50 =null;

        time_string_function_return time_string_function53 =null;

        time_string_function_return time_string_function55 =null;


        CommonTree BETWEEN37_tree=null;
        CommonTree char_literal38_tree=null;
        CommonTree NUMBER39_tree=null;
        CommonTree char_literal40_tree=null;
        CommonTree NUMBER41_tree=null;
        CommonTree char_literal42_tree=null;
        CommonTree BETWEEN44_tree=null;
        CommonTree char_literal45_tree=null;
        CommonTree char_literal47_tree=null;
        CommonTree char_literal49_tree=null;
        CommonTree BETWEEN51_tree=null;
        CommonTree char_literal52_tree=null;
        CommonTree char_literal54_tree=null;
        CommonTree char_literal56_tree=null;
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_33=new RewriteRuleTokenStream(adaptor,"token 33");
        RewriteRuleTokenStream stream_BETWEEN=new RewriteRuleTokenStream(adaptor,"token BETWEEN");
        RewriteRuleTokenStream stream_34=new RewriteRuleTokenStream(adaptor,"token 34");
        RewriteRuleTokenStream stream_NUMBER=new RewriteRuleTokenStream(adaptor,"token NUMBER");
        RewriteRuleSubtreeStream stream_time_string_function=new RewriteRuleSubtreeStream(adaptor,"rule time_string_function");
        RewriteRuleSubtreeStream stream_time_millis_function=new RewriteRuleSubtreeStream(adaptor,"rule time_millis_function");
        RewriteRuleSubtreeStream stream_path_function=new RewriteRuleSubtreeStream(adaptor,"rule path_function");
        try {
            // MessageFilter.g:138:2: ( path_function BETWEEN '(' NUMBER ',' NUMBER ')' -> ^( BETWEEN path_function NUMBER NUMBER ) | path_function BETWEEN '(' time_millis_function ',' time_millis_function ')' -> ^( BETWEEN path_function time_millis_function time_millis_function ) | path_function BETWEEN '(' time_string_function ',' time_string_function ')' -> ^( BETWEEN path_function time_string_function time_string_function ) )
            int alt7=3;
            int LA7_0 = input.LA(1);

            if ( (LA7_0==XPATH_FUN_NAME) ) {
                int LA7_1 = input.LA(2);

                if ( (LA7_1==33) ) {
                    int LA7_2 = input.LA(3);

                    if ( (LA7_2==STRING) ) {
                        int LA7_3 = input.LA(4);

                        if ( (LA7_3==34) ) {
                            int LA7_4 = input.LA(5);

                            if ( (LA7_4==BETWEEN) ) {
                                int LA7_5 = input.LA(6);

                                if ( (LA7_5==33) ) {
                                    switch ( input.LA(7) ) {
                                    case NUMBER:
                                        {
                                        alt7=1;
                                        }
                                        break;
                                    case TIME_MILLIS_FUN_NAME:
                                    case 35:
                                        {
                                        alt7=2;
                                        }
                                        break;
                                    case TIME_STRING_FUN_NAME:
                                        {
                                        alt7=3;
                                        }
                                        break;
                                    default:
                                        NoViableAltException nvae =
                                            new NoViableAltException("", 7, 6, input);

                                        throw nvae;

                                    }

                                }
                                else {
                                    NoViableAltException nvae =
                                        new NoViableAltException("", 7, 5, input);

                                    throw nvae;

                                }
                            }
                            else {
                                NoViableAltException nvae =
                                    new NoViableAltException("", 7, 4, input);

                                throw nvae;

                            }
                        }
                        else {
                            NoViableAltException nvae =
                                new NoViableAltException("", 7, 3, input);

                            throw nvae;

                        }
                    }
                    else {
                        NoViableAltException nvae =
                            new NoViableAltException("", 7, 2, input);

                        throw nvae;

                    }
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 7, 1, input);

                    throw nvae;

                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 7, 0, input);

                throw nvae;

            }
            switch (alt7) {
                case 1 :
                    // MessageFilter.g:138:4: path_function BETWEEN '(' NUMBER ',' NUMBER ')'
                    {
                    pushFollow(FOLLOW_path_function_in_between_predicate673);
                    path_function36=path_function();

                    state._fsp--;

                    stream_path_function.add(path_function36.getTree());

                    BETWEEN37=(Token)match(input,BETWEEN,FOLLOW_BETWEEN_in_between_predicate675);
                    stream_BETWEEN.add(BETWEEN37);


                    char_literal38=(Token)match(input,33,FOLLOW_33_in_between_predicate677);
                    stream_33.add(char_literal38);


                    NUMBER39=(Token)match(input,NUMBER,FOLLOW_NUMBER_in_between_predicate679);
                    stream_NUMBER.add(NUMBER39);


                    char_literal40=(Token)match(input,35,FOLLOW_35_in_between_predicate681);
                    stream_35.add(char_literal40);


                    NUMBER41=(Token)match(input,NUMBER,FOLLOW_NUMBER_in_between_predicate683);
                    stream_NUMBER.add(NUMBER41);


                    char_literal42=(Token)match(input,34,FOLLOW_34_in_between_predicate685);
                    stream_34.add(char_literal42);


                    // AST REWRITE
                    // elements: NUMBER, path_function, NUMBER, BETWEEN
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 139:4: -> ^( BETWEEN path_function NUMBER NUMBER )
                    {
                        // MessageFilter.g:139:7: ^( BETWEEN path_function NUMBER NUMBER )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        new BetweenTreeNode(stream_BETWEEN.nextToken())
                        , root_1);

                        adaptor.addChild(root_1, stream_path_function.nextTree());

                        adaptor.addChild(root_1,
                        new NumberTreeNode(stream_NUMBER.nextToken())
                        );

                        adaptor.addChild(root_1,
                        new NumberTreeNode(stream_NUMBER.nextToken())
                        );

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 2 :
                    // MessageFilter.g:140:3: path_function BETWEEN '(' time_millis_function ',' time_millis_function ')'
                    {
                    pushFollow(FOLLOW_path_function_in_between_predicate716);
                    path_function43=path_function();

                    state._fsp--;

                    stream_path_function.add(path_function43.getTree());

                    BETWEEN44=(Token)match(input,BETWEEN,FOLLOW_BETWEEN_in_between_predicate718);
                    stream_BETWEEN.add(BETWEEN44);


                    char_literal45=(Token)match(input,33,FOLLOW_33_in_between_predicate720);
                    stream_33.add(char_literal45);


                    pushFollow(FOLLOW_time_millis_function_in_between_predicate722);
                    time_millis_function46=time_millis_function();

                    state._fsp--;

                    stream_time_millis_function.add(time_millis_function46.getTree());

                    char_literal47=(Token)match(input,35,FOLLOW_35_in_between_predicate724);
                    stream_35.add(char_literal47);


                    pushFollow(FOLLOW_time_millis_function_in_between_predicate726);
                    time_millis_function48=time_millis_function();

                    state._fsp--;

                    stream_time_millis_function.add(time_millis_function48.getTree());

                    char_literal49=(Token)match(input,34,FOLLOW_34_in_between_predicate728);
                    stream_34.add(char_literal49);


                    // AST REWRITE
                    // elements: time_millis_function, BETWEEN, time_millis_function, path_function
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 141:4: -> ^( BETWEEN path_function time_millis_function time_millis_function )
                    {
                        // MessageFilter.g:141:7: ^( BETWEEN path_function time_millis_function time_millis_function )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        new BetweenTimeMillisTreeNode(stream_BETWEEN.nextToken())
                        , root_1);

                        adaptor.addChild(root_1, stream_path_function.nextTree());

                        adaptor.addChild(root_1, stream_time_millis_function.nextTree());

                        adaptor.addChild(root_1, stream_time_millis_function.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 3 :
                    // MessageFilter.g:142:3: path_function BETWEEN '(' time_string_function ',' time_string_function ')'
                    {
                    pushFollow(FOLLOW_path_function_in_between_predicate753);
                    path_function50=path_function();

                    state._fsp--;

                    stream_path_function.add(path_function50.getTree());

                    BETWEEN51=(Token)match(input,BETWEEN,FOLLOW_BETWEEN_in_between_predicate755);
                    stream_BETWEEN.add(BETWEEN51);


                    char_literal52=(Token)match(input,33,FOLLOW_33_in_between_predicate757);
                    stream_33.add(char_literal52);


                    pushFollow(FOLLOW_time_string_function_in_between_predicate759);
                    time_string_function53=time_string_function();

                    state._fsp--;

                    stream_time_string_function.add(time_string_function53.getTree());

                    char_literal54=(Token)match(input,35,FOLLOW_35_in_between_predicate761);
                    stream_35.add(char_literal54);


                    pushFollow(FOLLOW_time_string_function_in_between_predicate763);
                    time_string_function55=time_string_function();

                    state._fsp--;

                    stream_time_string_function.add(time_string_function55.getTree());

                    char_literal56=(Token)match(input,34,FOLLOW_34_in_between_predicate765);
                    stream_34.add(char_literal56);


                    // AST REWRITE
                    // elements: time_string_function, path_function, BETWEEN, time_string_function
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 143:4: -> ^( BETWEEN path_function time_string_function time_string_function )
                    {
                        // MessageFilter.g:143:7: ^( BETWEEN path_function time_string_function time_string_function )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        new BetweenTimeStringTreeNode(stream_BETWEEN.nextToken())
                        , root_1);

                        adaptor.addChild(root_1, stream_path_function.nextTree());

                        adaptor.addChild(root_1, stream_time_string_function.nextTree());

                        adaptor.addChild(root_1, stream_time_string_function.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "between_predicate"


    public static class in_predicate_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "in_predicate"
    // MessageFilter.g:146:1: in_predicate : ( path_function IN '(' STRING ( ',' STRING )* ')' -> ^( IN path_function ( STRING )+ ) | path_function IN '(' NUMBER ( ',' NUMBER )* ')' -> ^( IN path_function ( NUMBER )+ ) );
    public final in_predicate_return in_predicate() throws RecognitionException {
        in_predicate_return retval = new in_predicate_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token IN58=null;
        Token char_literal59=null;
        Token STRING60=null;
        Token char_literal61=null;
        Token STRING62=null;
        Token char_literal63=null;
        Token IN65=null;
        Token char_literal66=null;
        Token NUMBER67=null;
        Token char_literal68=null;
        Token NUMBER69=null;
        Token char_literal70=null;
        path_function_return path_function57 =null;

        path_function_return path_function64 =null;


        CommonTree IN58_tree=null;
        CommonTree char_literal59_tree=null;
        CommonTree STRING60_tree=null;
        CommonTree char_literal61_tree=null;
        CommonTree STRING62_tree=null;
        CommonTree char_literal63_tree=null;
        CommonTree IN65_tree=null;
        CommonTree char_literal66_tree=null;
        CommonTree NUMBER67_tree=null;
        CommonTree char_literal68_tree=null;
        CommonTree NUMBER69_tree=null;
        CommonTree char_literal70_tree=null;
        RewriteRuleTokenStream stream_IN=new RewriteRuleTokenStream(adaptor,"token IN");
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_33=new RewriteRuleTokenStream(adaptor,"token 33");
        RewriteRuleTokenStream stream_34=new RewriteRuleTokenStream(adaptor,"token 34");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");
        RewriteRuleTokenStream stream_NUMBER=new RewriteRuleTokenStream(adaptor,"token NUMBER");
        RewriteRuleSubtreeStream stream_path_function=new RewriteRuleSubtreeStream(adaptor,"rule path_function");
        try {
            // MessageFilter.g:147:2: ( path_function IN '(' STRING ( ',' STRING )* ')' -> ^( IN path_function ( STRING )+ ) | path_function IN '(' NUMBER ( ',' NUMBER )* ')' -> ^( IN path_function ( NUMBER )+ ) )
            int alt10=2;
            int LA10_0 = input.LA(1);

            if ( (LA10_0==XPATH_FUN_NAME) ) {
                int LA10_1 = input.LA(2);

                if ( (LA10_1==33) ) {
                    int LA10_2 = input.LA(3);

                    if ( (LA10_2==STRING) ) {
                        int LA10_3 = input.LA(4);

                        if ( (LA10_3==34) ) {
                            int LA10_4 = input.LA(5);

                            if ( (LA10_4==IN) ) {
                                int LA10_5 = input.LA(6);

                                if ( (LA10_5==33) ) {
                                    int LA10_6 = input.LA(7);

                                    if ( (LA10_6==STRING) ) {
                                        alt10=1;
                                    }
                                    else if ( (LA10_6==NUMBER) ) {
                                        alt10=2;
                                    }
                                    else {
                                        NoViableAltException nvae =
                                            new NoViableAltException("", 10, 6, input);

                                        throw nvae;

                                    }
                                }
                                else {
                                    NoViableAltException nvae =
                                        new NoViableAltException("", 10, 5, input);

                                    throw nvae;

                                }
                            }
                            else {
                                NoViableAltException nvae =
                                    new NoViableAltException("", 10, 4, input);

                                throw nvae;

                            }
                        }
                        else {
                            NoViableAltException nvae =
                                new NoViableAltException("", 10, 3, input);

                            throw nvae;

                        }
                    }
                    else {
                        NoViableAltException nvae =
                            new NoViableAltException("", 10, 2, input);

                        throw nvae;

                    }
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 1, input);

                    throw nvae;

                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 10, 0, input);

                throw nvae;

            }
            switch (alt10) {
                case 1 :
                    // MessageFilter.g:147:4: path_function IN '(' STRING ( ',' STRING )* ')'
                    {
                    pushFollow(FOLLOW_path_function_in_in_predicate796);
                    path_function57=path_function();

                    state._fsp--;

                    stream_path_function.add(path_function57.getTree());

                    IN58=(Token)match(input,IN,FOLLOW_IN_in_in_predicate798);
                    stream_IN.add(IN58);


                    char_literal59=(Token)match(input,33,FOLLOW_33_in_in_predicate800);
                    stream_33.add(char_literal59);


                    STRING60=(Token)match(input,STRING,FOLLOW_STRING_in_in_predicate802);
                    stream_STRING.add(STRING60);


                    // MessageFilter.g:147:32: ( ',' STRING )*
                    loop8:
                    do {
                        int alt8=2;
                        int LA8_0 = input.LA(1);

                        if ( (LA8_0==35) ) {
                            alt8=1;
                        }


                        switch (alt8) {
                    	case 1 :
                    	    // MessageFilter.g:147:33: ',' STRING
                    	    {
                    	    char_literal61=(Token)match(input,35,FOLLOW_35_in_in_predicate805);
                    	    stream_35.add(char_literal61);


                    	    STRING62=(Token)match(input,STRING,FOLLOW_STRING_in_in_predicate807);
                    	    stream_STRING.add(STRING62);


                    	    }
                    	    break;

                    	default :
                    	    break loop8;
                        }
                    } while (true);


                    char_literal63=(Token)match(input,34,FOLLOW_34_in_in_predicate811);
                    stream_34.add(char_literal63);


                    // AST REWRITE
                    // elements: IN, STRING, path_function
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 147:50: -> ^( IN path_function ( STRING )+ )
                    {
                        // MessageFilter.g:147:53: ^( IN path_function ( STRING )+ )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        new StringInTreeNode(stream_IN.nextToken())
                        , root_1);

                        adaptor.addChild(root_1, stream_path_function.nextTree());

                        if ( !(stream_STRING.hasNext()) ) {
                            throw new RewriteEarlyExitException();
                        }
                        while ( stream_STRING.hasNext() ) {
                            adaptor.addChild(root_1,
                            new StringTreeNode(stream_STRING.nextToken())
                            );

                        }
                        stream_STRING.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 2 :
                    // MessageFilter.g:148:3: path_function IN '(' NUMBER ( ',' NUMBER )* ')'
                    {
                    pushFollow(FOLLOW_path_function_in_in_predicate836);
                    path_function64=path_function();

                    state._fsp--;

                    stream_path_function.add(path_function64.getTree());

                    IN65=(Token)match(input,IN,FOLLOW_IN_in_in_predicate838);
                    stream_IN.add(IN65);


                    char_literal66=(Token)match(input,33,FOLLOW_33_in_in_predicate840);
                    stream_33.add(char_literal66);


                    NUMBER67=(Token)match(input,NUMBER,FOLLOW_NUMBER_in_in_predicate842);
                    stream_NUMBER.add(NUMBER67);


                    // MessageFilter.g:148:31: ( ',' NUMBER )*
                    loop9:
                    do {
                        int alt9=2;
                        int LA9_0 = input.LA(1);

                        if ( (LA9_0==35) ) {
                            alt9=1;
                        }


                        switch (alt9) {
                    	case 1 :
                    	    // MessageFilter.g:148:32: ',' NUMBER
                    	    {
                    	    char_literal68=(Token)match(input,35,FOLLOW_35_in_in_predicate845);
                    	    stream_35.add(char_literal68);


                    	    NUMBER69=(Token)match(input,NUMBER,FOLLOW_NUMBER_in_in_predicate847);
                    	    stream_NUMBER.add(NUMBER69);


                    	    }
                    	    break;

                    	default :
                    	    break loop9;
                        }
                    } while (true);


                    char_literal70=(Token)match(input,34,FOLLOW_34_in_in_predicate851);
                    stream_34.add(char_literal70);


                    // AST REWRITE
                    // elements: NUMBER, path_function, IN
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 148:49: -> ^( IN path_function ( NUMBER )+ )
                    {
                        // MessageFilter.g:148:52: ^( IN path_function ( NUMBER )+ )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        new NumericInTreeNode(stream_IN.nextToken())
                        , root_1);

                        adaptor.addChild(root_1, stream_path_function.nextTree());

                        if ( !(stream_NUMBER.hasNext()) ) {
                            throw new RewriteEarlyExitException();
                        }
                        while ( stream_NUMBER.hasNext() ) {
                            adaptor.addChild(root_1,
                            new NumberTreeNode(stream_NUMBER.nextToken())
                            );

                        }
                        stream_NUMBER.reset();

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "in_predicate"


    public static class null_predicate_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "null_predicate"
    // MessageFilter.g:151:1: null_predicate : path_function IS NULL -> ^( NULL path_function ) ;
    public final null_predicate_return null_predicate() throws RecognitionException {
        null_predicate_return retval = new null_predicate_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token IS72=null;
        Token NULL73=null;
        path_function_return path_function71 =null;


        CommonTree IS72_tree=null;
        CommonTree NULL73_tree=null;
        RewriteRuleTokenStream stream_IS=new RewriteRuleTokenStream(adaptor,"token IS");
        RewriteRuleTokenStream stream_NULL=new RewriteRuleTokenStream(adaptor,"token NULL");
        RewriteRuleSubtreeStream stream_path_function=new RewriteRuleSubtreeStream(adaptor,"rule path_function");
        try {
            // MessageFilter.g:152:2: ( path_function IS NULL -> ^( NULL path_function ) )
            // MessageFilter.g:152:4: path_function IS NULL
            {
            pushFollow(FOLLOW_path_function_in_null_predicate881);
            path_function71=path_function();

            state._fsp--;

            stream_path_function.add(path_function71.getTree());

            IS72=(Token)match(input,IS,FOLLOW_IS_in_null_predicate883);
            stream_IS.add(IS72);


            NULL73=(Token)match(input,NULL,FOLLOW_NULL_in_null_predicate885);
            stream_NULL.add(NULL73);


            // AST REWRITE
            // elements: path_function, NULL
            // token labels:
            // rule labels: retval
            // token list labels:
            // rule list labels:
            // wildcard labels:
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 152:26: -> ^( NULL path_function )
            {
                // MessageFilter.g:152:29: ^( NULL path_function )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                new NullTreeNode(stream_NULL.nextToken())
                , root_1);

                adaptor.addChild(root_1, stream_path_function.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;

            }

            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "null_predicate"


    public static class regex_predicate_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "regex_predicate"
    // MessageFilter.g:155:1: regex_predicate : path_function MATCHES STRING -> ^( MATCHES path_function STRING ) ;
    public final regex_predicate_return regex_predicate() throws RecognitionException {
        regex_predicate_return retval = new regex_predicate_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token MATCHES75=null;
        Token STRING76=null;
        path_function_return path_function74 =null;


        CommonTree MATCHES75_tree=null;
        CommonTree STRING76_tree=null;
        RewriteRuleTokenStream stream_MATCHES=new RewriteRuleTokenStream(adaptor,"token MATCHES");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");
        RewriteRuleSubtreeStream stream_path_function=new RewriteRuleSubtreeStream(adaptor,"rule path_function");
        try {
            // MessageFilter.g:156:2: ( path_function MATCHES STRING -> ^( MATCHES path_function STRING ) )
            // MessageFilter.g:156:6: path_function MATCHES STRING
            {
            pushFollow(FOLLOW_path_function_in_regex_predicate909);
            path_function74=path_function();

            state._fsp--;

            stream_path_function.add(path_function74.getTree());

            MATCHES75=(Token)match(input,MATCHES,FOLLOW_MATCHES_in_regex_predicate911);
            stream_MATCHES.add(MATCHES75);


            STRING76=(Token)match(input,STRING,FOLLOW_STRING_in_regex_predicate913);
            stream_STRING.add(STRING76);


            // AST REWRITE
            // elements: MATCHES, path_function, STRING
            // token labels:
            // rule labels: retval
            // token list labels:
            // rule list labels:
            // wildcard labels:
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 156:35: -> ^( MATCHES path_function STRING )
            {
                // MessageFilter.g:156:38: ^( MATCHES path_function STRING )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                new MatchesTreeNode(stream_MATCHES.nextToken())
                , root_1);

                adaptor.addChild(root_1, stream_path_function.nextTree());

                adaptor.addChild(root_1,
                new StringTreeNode(stream_STRING.nextToken())
                );

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;

            }

            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "regex_predicate"


    public static class exists_predicate_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "exists_predicate"
    // MessageFilter.g:159:1: exists_predicate : ( path_function EXISTS -> ^( EXISTS path_function ) | EXISTS path_function -> ^( EXISTS path_function ) );
    public final exists_predicate_return exists_predicate() throws RecognitionException {
        exists_predicate_return retval = new exists_predicate_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token EXISTS78=null;
        Token EXISTS79=null;
        path_function_return path_function77 =null;

        path_function_return path_function80 =null;


        CommonTree EXISTS78_tree=null;
        CommonTree EXISTS79_tree=null;
        RewriteRuleTokenStream stream_EXISTS=new RewriteRuleTokenStream(adaptor,"token EXISTS");
        RewriteRuleSubtreeStream stream_path_function=new RewriteRuleSubtreeStream(adaptor,"rule path_function");
        try {
            // MessageFilter.g:160:2: ( path_function EXISTS -> ^( EXISTS path_function ) | EXISTS path_function -> ^( EXISTS path_function ) )
            int alt11=2;
            int LA11_0 = input.LA(1);

            if ( (LA11_0==XPATH_FUN_NAME) ) {
                alt11=1;
            }
            else if ( (LA11_0==EXISTS) ) {
                alt11=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 11, 0, input);

                throw nvae;

            }
            switch (alt11) {
                case 1 :
                    // MessageFilter.g:160:4: path_function EXISTS
                    {
                    pushFollow(FOLLOW_path_function_in_exists_predicate940);
                    path_function77=path_function();

                    state._fsp--;

                    stream_path_function.add(path_function77.getTree());

                    EXISTS78=(Token)match(input,EXISTS,FOLLOW_EXISTS_in_exists_predicate942);
                    stream_EXISTS.add(EXISTS78);


                    // AST REWRITE
                    // elements: EXISTS, path_function
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 160:25: -> ^( EXISTS path_function )
                    {
                        // MessageFilter.g:160:28: ^( EXISTS path_function )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        new ExistsTreeNode(stream_EXISTS.nextToken())
                        , root_1);

                        adaptor.addChild(root_1, stream_path_function.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 2 :
                    // MessageFilter.g:161:3: EXISTS path_function
                    {
                    EXISTS79=(Token)match(input,EXISTS,FOLLOW_EXISTS_in_exists_predicate959);
                    stream_EXISTS.add(EXISTS79);


                    pushFollow(FOLLOW_path_function_in_exists_predicate961);
                    path_function80=path_function();

                    state._fsp--;

                    stream_path_function.add(path_function80.getTree());

                    // AST REWRITE
                    // elements: path_function, EXISTS
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 161:24: -> ^( EXISTS path_function )
                    {
                        // MessageFilter.g:161:27: ^( EXISTS path_function )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        new ExistsTreeNode(stream_EXISTS.nextToken())
                        , root_1);

                        adaptor.addChild(root_1, stream_path_function.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "exists_predicate"


    public static class path_function_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "path_function"
    // MessageFilter.g:164:1: path_function : XPATH_FUN_NAME '(' STRING ')' -> ^( XPATH_FUN_NAME STRING ) ;
    public final path_function_return path_function() throws RecognitionException {
        path_function_return retval = new path_function_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token XPATH_FUN_NAME81=null;
        Token char_literal82=null;
        Token STRING83=null;
        Token char_literal84=null;

        CommonTree XPATH_FUN_NAME81_tree=null;
        CommonTree char_literal82_tree=null;
        CommonTree STRING83_tree=null;
        CommonTree char_literal84_tree=null;
        RewriteRuleTokenStream stream_XPATH_FUN_NAME=new RewriteRuleTokenStream(adaptor,"token XPATH_FUN_NAME");
        RewriteRuleTokenStream stream_33=new RewriteRuleTokenStream(adaptor,"token 33");
        RewriteRuleTokenStream stream_34=new RewriteRuleTokenStream(adaptor,"token 34");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            // MessageFilter.g:165:2: ( XPATH_FUN_NAME '(' STRING ')' -> ^( XPATH_FUN_NAME STRING ) )
            // MessageFilter.g:165:4: XPATH_FUN_NAME '(' STRING ')'
            {
            XPATH_FUN_NAME81=(Token)match(input,XPATH_FUN_NAME,FOLLOW_XPATH_FUN_NAME_in_path_function990);
            stream_XPATH_FUN_NAME.add(XPATH_FUN_NAME81);


            char_literal82=(Token)match(input,33,FOLLOW_33_in_path_function992);
            stream_33.add(char_literal82);


            STRING83=(Token)match(input,STRING,FOLLOW_STRING_in_path_function994);
            stream_STRING.add(STRING83);


            char_literal84=(Token)match(input,34,FOLLOW_34_in_path_function996);
            stream_34.add(char_literal84);


            // AST REWRITE
            // elements: XPATH_FUN_NAME, STRING
            // token labels:
            // rule labels: retval
            // token list labels:
            // rule list labels:
            // wildcard labels:
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 165:34: -> ^( XPATH_FUN_NAME STRING )
            {
                // MessageFilter.g:165:37: ^( XPATH_FUN_NAME STRING )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                new XPathTreeNode(stream_XPATH_FUN_NAME.nextToken())
                , root_1);

                adaptor.addChild(root_1,
                new StringTreeNode(stream_STRING.nextToken())
                );

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;

            }

            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "path_function"


    public static class value_function_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "value_function"
    // MessageFilter.g:168:1: value_function : ( equality_value_function | compariable_value_function );
    public final value_function_return value_function() throws RecognitionException {
        value_function_return retval = new value_function_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        equality_value_function_return equality_value_function85 =null;

        compariable_value_function_return compariable_value_function86 =null;



        try {
            // MessageFilter.g:169:2: ( equality_value_function | compariable_value_function )
            int alt12=2;
            int LA12_0 = input.LA(1);

            if ( (LA12_0==FALSE||LA12_0==NULL||LA12_0==STRING||LA12_0==TRUE||LA12_0==XPATH_FUN_NAME) ) {
                alt12=1;
            }
            else if ( (LA12_0==EOF||LA12_0==AND||LA12_0==NUMBER||LA12_0==OR||(LA12_0 >= TIME_MILLIS_FUN_NAME && LA12_0 <= TIME_STRING_FUN_NAME)||(LA12_0 >= 34 && LA12_0 <= 35)) ) {
                alt12=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 12, 0, input);

                throw nvae;

            }
            switch (alt12) {
                case 1 :
                    // MessageFilter.g:169:4: equality_value_function
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_equality_value_function_in_value_function1022);
                    equality_value_function85=equality_value_function();

                    state._fsp--;

                    adaptor.addChild(root_0, equality_value_function85.getTree());

                    }
                    break;
                case 2 :
                    // MessageFilter.g:169:30: compariable_value_function
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_compariable_value_function_in_value_function1026);
                    compariable_value_function86=compariable_value_function();

                    state._fsp--;

                    adaptor.addChild(root_0, compariable_value_function86.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "value_function"


    public static class equality_value_function_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "equality_value_function"
    // MessageFilter.g:172:1: equality_value_function : ( STRING -> STRING | TRUE -> TRUE | FALSE -> FALSE | NULL -> NULL | path_function );
    public final equality_value_function_return equality_value_function() throws RecognitionException {
        equality_value_function_return retval = new equality_value_function_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token STRING87=null;
        Token TRUE88=null;
        Token FALSE89=null;
        Token NULL90=null;
        path_function_return path_function91 =null;


        CommonTree STRING87_tree=null;
        CommonTree TRUE88_tree=null;
        CommonTree FALSE89_tree=null;
        CommonTree NULL90_tree=null;
        RewriteRuleTokenStream stream_FALSE=new RewriteRuleTokenStream(adaptor,"token FALSE");
        RewriteRuleTokenStream stream_TRUE=new RewriteRuleTokenStream(adaptor,"token TRUE");
        RewriteRuleTokenStream stream_NULL=new RewriteRuleTokenStream(adaptor,"token NULL");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            // MessageFilter.g:173:2: ( STRING -> STRING | TRUE -> TRUE | FALSE -> FALSE | NULL -> NULL | path_function )
            int alt13=5;
            switch ( input.LA(1) ) {
            case STRING:
                {
                alt13=1;
                }
                break;
            case TRUE:
                {
                alt13=2;
                }
                break;
            case FALSE:
                {
                alt13=3;
                }
                break;
            case NULL:
                {
                alt13=4;
                }
                break;
            case XPATH_FUN_NAME:
                {
                alt13=5;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 13, 0, input);

                throw nvae;

            }

            switch (alt13) {
                case 1 :
                    // MessageFilter.g:173:4: STRING
                    {
                    STRING87=(Token)match(input,STRING,FOLLOW_STRING_in_equality_value_function1038);
                    stream_STRING.add(STRING87);


                    // AST REWRITE
                    // elements: STRING
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 173:11: -> STRING
                    {
                        adaptor.addChild(root_0,
                        new StringTreeNode(stream_STRING.nextToken())
                        );

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 2 :
                    // MessageFilter.g:174:3: TRUE
                    {
                    TRUE88=(Token)match(input,TRUE,FOLLOW_TRUE_in_equality_value_function1051);
                    stream_TRUE.add(TRUE88);


                    // AST REWRITE
                    // elements: TRUE
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 174:8: -> TRUE
                    {
                        adaptor.addChild(root_0,
                        new TrueValueTreeNode(stream_TRUE.nextToken())
                        );

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 3 :
                    // MessageFilter.g:175:3: FALSE
                    {
                    FALSE89=(Token)match(input,FALSE,FOLLOW_FALSE_in_equality_value_function1064);
                    stream_FALSE.add(FALSE89);


                    // AST REWRITE
                    // elements: FALSE
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 175:9: -> FALSE
                    {
                        adaptor.addChild(root_0,
                        new FalseValueTreeNode(stream_FALSE.nextToken())
                        );

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 4 :
                    // MessageFilter.g:176:3: NULL
                    {
                    NULL90=(Token)match(input,NULL,FOLLOW_NULL_in_equality_value_function1077);
                    stream_NULL.add(NULL90);


                    // AST REWRITE
                    // elements: NULL
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 176:8: -> NULL
                    {
                        adaptor.addChild(root_0,
                        new NullValueTreeNode(stream_NULL.nextToken())
                        );

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 5 :
                    // MessageFilter.g:177:3: path_function
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_path_function_in_equality_value_function1090);
                    path_function91=path_function();

                    state._fsp--;

                    adaptor.addChild(root_0, path_function91.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "equality_value_function"


    public static class compariable_value_function_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "compariable_value_function"
    // MessageFilter.g:180:1: compariable_value_function : ( NUMBER -> NUMBER | time_millis_function | time_string_function );
    public final compariable_value_function_return compariable_value_function() throws RecognitionException {
        compariable_value_function_return retval = new compariable_value_function_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token NUMBER92=null;
        time_millis_function_return time_millis_function93 =null;

        time_string_function_return time_string_function94 =null;


        CommonTree NUMBER92_tree=null;
        RewriteRuleTokenStream stream_NUMBER=new RewriteRuleTokenStream(adaptor,"token NUMBER");

        try {
            // MessageFilter.g:181:2: ( NUMBER -> NUMBER | time_millis_function | time_string_function )
            int alt14=3;
            switch ( input.LA(1) ) {
            case NUMBER:
                {
                alt14=1;
                }
                break;
            case EOF:
            case AND:
            case OR:
            case TIME_MILLIS_FUN_NAME:
            case 34:
            case 35:
                {
                alt14=2;
                }
                break;
            case TIME_STRING_FUN_NAME:
                {
                alt14=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 14, 0, input);

                throw nvae;

            }

            switch (alt14) {
                case 1 :
                    // MessageFilter.g:181:4: NUMBER
                    {
                    NUMBER92=(Token)match(input,NUMBER,FOLLOW_NUMBER_in_compariable_value_function1102);
                    stream_NUMBER.add(NUMBER92);


                    // AST REWRITE
                    // elements: NUMBER
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 181:11: -> NUMBER
                    {
                        adaptor.addChild(root_0,
                        new NumberTreeNode(stream_NUMBER.nextToken())
                        );

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 2 :
                    // MessageFilter.g:182:3: time_millis_function
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_time_millis_function_in_compariable_value_function1114);
                    time_millis_function93=time_millis_function();

                    state._fsp--;

                    adaptor.addChild(root_0, time_millis_function93.getTree());

                    }
                    break;
                case 3 :
                    // MessageFilter.g:183:3: time_string_function
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_time_string_function_in_compariable_value_function1120);
                    time_string_function94=time_string_function();

                    state._fsp--;

                    adaptor.addChild(root_0, time_string_function94.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "compariable_value_function"


    public static class time_millis_function_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "time_millis_function"
    // MessageFilter.g:186:1: time_millis_function : ( TIME_MILLIS_FUN_NAME '(' STRING ',' STRING ')' -> ^( TIME_MILLIS_FUN_NAME STRING STRING ) |);
    public final time_millis_function_return time_millis_function() throws RecognitionException {
        time_millis_function_return retval = new time_millis_function_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token TIME_MILLIS_FUN_NAME95=null;
        Token char_literal96=null;
        Token STRING97=null;
        Token char_literal98=null;
        Token STRING99=null;
        Token char_literal100=null;

        CommonTree TIME_MILLIS_FUN_NAME95_tree=null;
        CommonTree char_literal96_tree=null;
        CommonTree STRING97_tree=null;
        CommonTree char_literal98_tree=null;
        CommonTree STRING99_tree=null;
        CommonTree char_literal100_tree=null;
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_33=new RewriteRuleTokenStream(adaptor,"token 33");
        RewriteRuleTokenStream stream_34=new RewriteRuleTokenStream(adaptor,"token 34");
        RewriteRuleTokenStream stream_TIME_MILLIS_FUN_NAME=new RewriteRuleTokenStream(adaptor,"token TIME_MILLIS_FUN_NAME");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            // MessageFilter.g:187:2: ( TIME_MILLIS_FUN_NAME '(' STRING ',' STRING ')' -> ^( TIME_MILLIS_FUN_NAME STRING STRING ) |)
            int alt15=2;
            int LA15_0 = input.LA(1);

            if ( (LA15_0==TIME_MILLIS_FUN_NAME) ) {
                alt15=1;
            }
            else if ( (LA15_0==EOF||LA15_0==AND||LA15_0==OR||(LA15_0 >= 34 && LA15_0 <= 35)) ) {
                alt15=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 15, 0, input);

                throw nvae;

            }
            switch (alt15) {
                case 1 :
                    // MessageFilter.g:187:4: TIME_MILLIS_FUN_NAME '(' STRING ',' STRING ')'
                    {
                    TIME_MILLIS_FUN_NAME95=(Token)match(input,TIME_MILLIS_FUN_NAME,FOLLOW_TIME_MILLIS_FUN_NAME_in_time_millis_function1132);
                    stream_TIME_MILLIS_FUN_NAME.add(TIME_MILLIS_FUN_NAME95);


                    char_literal96=(Token)match(input,33,FOLLOW_33_in_time_millis_function1134);
                    stream_33.add(char_literal96);


                    STRING97=(Token)match(input,STRING,FOLLOW_STRING_in_time_millis_function1136);
                    stream_STRING.add(STRING97);


                    char_literal98=(Token)match(input,35,FOLLOW_35_in_time_millis_function1138);
                    stream_35.add(char_literal98);


                    STRING99=(Token)match(input,STRING,FOLLOW_STRING_in_time_millis_function1140);
                    stream_STRING.add(STRING99);


                    char_literal100=(Token)match(input,34,FOLLOW_34_in_time_millis_function1142);
                    stream_34.add(char_literal100);


                    // AST REWRITE
                    // elements: TIME_MILLIS_FUN_NAME, STRING, STRING
                    // token labels:
                    // rule labels: retval
                    // token list labels:
                    // rule list labels:
                    // wildcard labels:
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 187:51: -> ^( TIME_MILLIS_FUN_NAME STRING STRING )
                    {
                        // MessageFilter.g:187:54: ^( TIME_MILLIS_FUN_NAME STRING STRING )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        new TimeMillisValueTreeNode(stream_TIME_MILLIS_FUN_NAME.nextToken())
                        , root_1);

                        adaptor.addChild(root_1,
                        new StringTreeNode(stream_STRING.nextToken())
                        );

                        adaptor.addChild(root_1,
                        new StringTreeNode(stream_STRING.nextToken())
                        );

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 2 :
                    // MessageFilter.g:188:2:
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    }
                    break;

            }
            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "time_millis_function"


    public static class time_string_function_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "time_string_function"
    // MessageFilter.g:193:1: time_string_function : TIME_STRING_FUN_NAME '(' STRING ',' STRING ',' STRING ')' -> ^( TIME_STRING_FUN_NAME STRING STRING STRING ) ;
    public final time_string_function_return time_string_function() throws RecognitionException {
        time_string_function_return retval = new time_string_function_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token TIME_STRING_FUN_NAME101=null;
        Token char_literal102=null;
        Token STRING103=null;
        Token char_literal104=null;
        Token STRING105=null;
        Token char_literal106=null;
        Token STRING107=null;
        Token char_literal108=null;

        CommonTree TIME_STRING_FUN_NAME101_tree=null;
        CommonTree char_literal102_tree=null;
        CommonTree STRING103_tree=null;
        CommonTree char_literal104_tree=null;
        CommonTree STRING105_tree=null;
        CommonTree char_literal106_tree=null;
        CommonTree STRING107_tree=null;
        CommonTree char_literal108_tree=null;
        RewriteRuleTokenStream stream_35=new RewriteRuleTokenStream(adaptor,"token 35");
        RewriteRuleTokenStream stream_TIME_STRING_FUN_NAME=new RewriteRuleTokenStream(adaptor,"token TIME_STRING_FUN_NAME");
        RewriteRuleTokenStream stream_33=new RewriteRuleTokenStream(adaptor,"token 33");
        RewriteRuleTokenStream stream_34=new RewriteRuleTokenStream(adaptor,"token 34");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            // MessageFilter.g:194:2: ( TIME_STRING_FUN_NAME '(' STRING ',' STRING ',' STRING ')' -> ^( TIME_STRING_FUN_NAME STRING STRING STRING ) )
            // MessageFilter.g:194:5: TIME_STRING_FUN_NAME '(' STRING ',' STRING ',' STRING ')'
            {
            TIME_STRING_FUN_NAME101=(Token)match(input,TIME_STRING_FUN_NAME,FOLLOW_TIME_STRING_FUN_NAME_in_time_string_function1179);  
            stream_TIME_STRING_FUN_NAME.add(TIME_STRING_FUN_NAME101);


            char_literal102=(Token)match(input,33,FOLLOW_33_in_time_string_function1181);  
            stream_33.add(char_literal102);


            STRING103=(Token)match(input,STRING,FOLLOW_STRING_in_time_string_function1183);  
            stream_STRING.add(STRING103);


            char_literal104=(Token)match(input,35,FOLLOW_35_in_time_string_function1185);  
            stream_35.add(char_literal104);


            STRING105=(Token)match(input,STRING,FOLLOW_STRING_in_time_string_function1187);  
            stream_STRING.add(STRING105);


            char_literal106=(Token)match(input,35,FOLLOW_35_in_time_string_function1189);  
            stream_35.add(char_literal106);


            STRING107=(Token)match(input,STRING,FOLLOW_STRING_in_time_string_function1191);  
            stream_STRING.add(STRING107);


            char_literal108=(Token)match(input,34,FOLLOW_34_in_time_string_function1193);  
            stream_34.add(char_literal108);


            // AST REWRITE
            // elements: STRING, TIME_STRING_FUN_NAME, STRING, STRING
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 194:63: -> ^( TIME_STRING_FUN_NAME STRING STRING STRING )
            {
                // MessageFilter.g:194:66: ^( TIME_STRING_FUN_NAME STRING STRING STRING )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                new TimeStringValueTreeNode(stream_TIME_STRING_FUN_NAME.nextToken())
                , root_1);

                adaptor.addChild(root_1, 
                new StringTreeNode(stream_STRING.nextToken())
                );

                adaptor.addChild(root_1, 
                new StringTreeNode(stream_STRING.nextToken())
                );

                adaptor.addChild(root_1, 
                new StringTreeNode(stream_STRING.nextToken())
                );

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;

            }

            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "time_string_function"

    // Delegated rules


 

    public static final BitSet FOLLOW_boolean_expr_in_filter323 = new BitSet(new long[]{0x0000000002000002L});
    public static final BitSet FOLLOW_OR_in_filter330 = new BitSet(new long[]{0x0000000320100A00L});
    public static final BitSet FOLLOW_boolean_expr_in_filter334 = new BitSet(new long[]{0x0000000002000002L});
    public static final BitSet FOLLOW_EOF_in_filter354 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_boolean_factor_in_boolean_expr370 = new BitSet(new long[]{0x0000000000000012L});
    public static final BitSet FOLLOW_AND_in_boolean_expr377 = new BitSet(new long[]{0x0000000320100A00L});
    public static final BitSet FOLLOW_boolean_factor_in_boolean_expr381 = new BitSet(new long[]{0x0000000000000012L});
    public static final BitSet FOLLOW_predicate_in_boolean_factor414 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NOT_in_boolean_factor423 = new BitSet(new long[]{0x0000000320000A00L});
    public static final BitSet FOLLOW_predicate_in_boolean_factor425 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_33_in_predicate448 = new BitSet(new long[]{0x0000000320100A00L});
    public static final BitSet FOLLOW_filter_in_predicate450 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_34_in_predicate452 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_comparison_function_in_predicate463 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_between_predicate_in_predicate469 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_in_predicate_in_predicate475 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_null_predicate_in_predicate481 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_regex_predicate_in_predicate487 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_exists_predicate_in_predicate493 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_TRUE_in_predicate499 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_FALSE_in_predicate511 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_function_in_comparison_function529 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_EQUALS_in_comparison_function531 = new BitSet(new long[]{0x000000013CC00800L});
    public static final BitSet FOLLOW_value_function_in_comparison_function533 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_function_in_comparison_function552 = new BitSet(new long[]{0x0000000000200000L});
    public static final BitSet FOLLOW_NOT_EQUALS_in_comparison_function554 = new BitSet(new long[]{0x000000013CC00800L});
    public static final BitSet FOLLOW_value_function_in_comparison_function556 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_function_in_comparison_function575 = new BitSet(new long[]{0x0000000000002000L});
    public static final BitSet FOLLOW_GT_in_comparison_function577 = new BitSet(new long[]{0x0000000018800000L});
    public static final BitSet FOLLOW_compariable_value_function_in_comparison_function579 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_function_in_comparison_function598 = new BitSet(new long[]{0x0000000000001000L});
    public static final BitSet FOLLOW_GE_in_comparison_function600 = new BitSet(new long[]{0x0000000018800000L});
    public static final BitSet FOLLOW_compariable_value_function_in_comparison_function602 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_function_in_comparison_function621 = new BitSet(new long[]{0x0000000000040000L});
    public static final BitSet FOLLOW_LT_in_comparison_function623 = new BitSet(new long[]{0x0000000018800000L});
    public static final BitSet FOLLOW_compariable_value_function_in_comparison_function625 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_function_in_comparison_function644 = new BitSet(new long[]{0x0000000000020000L});
    public static final BitSet FOLLOW_LE_in_comparison_function646 = new BitSet(new long[]{0x0000000018800000L});
    public static final BitSet FOLLOW_compariable_value_function_in_comparison_function648 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_function_in_between_predicate673 = new BitSet(new long[]{0x0000000000000020L});
    public static final BitSet FOLLOW_BETWEEN_in_between_predicate675 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_33_in_between_predicate677 = new BitSet(new long[]{0x0000000000800000L});
    public static final BitSet FOLLOW_NUMBER_in_between_predicate679 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_between_predicate681 = new BitSet(new long[]{0x0000000000800000L});
    public static final BitSet FOLLOW_NUMBER_in_between_predicate683 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_34_in_between_predicate685 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_function_in_between_predicate716 = new BitSet(new long[]{0x0000000000000020L});
    public static final BitSet FOLLOW_BETWEEN_in_between_predicate718 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_33_in_between_predicate720 = new BitSet(new long[]{0x0000000808000000L});
    public static final BitSet FOLLOW_time_millis_function_in_between_predicate722 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_between_predicate724 = new BitSet(new long[]{0x0000000408000000L});
    public static final BitSet FOLLOW_time_millis_function_in_between_predicate726 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_34_in_between_predicate728 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_function_in_between_predicate753 = new BitSet(new long[]{0x0000000000000020L});
    public static final BitSet FOLLOW_BETWEEN_in_between_predicate755 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_33_in_between_predicate757 = new BitSet(new long[]{0x0000000010000000L});
    public static final BitSet FOLLOW_time_string_function_in_between_predicate759 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_between_predicate761 = new BitSet(new long[]{0x0000000010000000L});
    public static final BitSet FOLLOW_time_string_function_in_between_predicate763 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_34_in_between_predicate765 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_function_in_in_predicate796 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_IN_in_in_predicate798 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_33_in_in_predicate800 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_STRING_in_in_predicate802 = new BitSet(new long[]{0x0000000C00000000L});
    public static final BitSet FOLLOW_35_in_in_predicate805 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_STRING_in_in_predicate807 = new BitSet(new long[]{0x0000000C00000000L});
    public static final BitSet FOLLOW_34_in_in_predicate811 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_function_in_in_predicate836 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_IN_in_in_predicate838 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_33_in_in_predicate840 = new BitSet(new long[]{0x0000000000800000L});
    public static final BitSet FOLLOW_NUMBER_in_in_predicate842 = new BitSet(new long[]{0x0000000C00000000L});
    public static final BitSet FOLLOW_35_in_in_predicate845 = new BitSet(new long[]{0x0000000000800000L});
    public static final BitSet FOLLOW_NUMBER_in_in_predicate847 = new BitSet(new long[]{0x0000000C00000000L});
    public static final BitSet FOLLOW_34_in_in_predicate851 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_function_in_null_predicate881 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_IS_in_null_predicate883 = new BitSet(new long[]{0x0000000000400000L});
    public static final BitSet FOLLOW_NULL_in_null_predicate885 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_function_in_regex_predicate909 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_MATCHES_in_regex_predicate911 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_STRING_in_regex_predicate913 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_function_in_exists_predicate940 = new BitSet(new long[]{0x0000000000000200L});
    public static final BitSet FOLLOW_EXISTS_in_exists_predicate942 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_EXISTS_in_exists_predicate959 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_path_function_in_exists_predicate961 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_XPATH_FUN_NAME_in_path_function990 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_33_in_path_function992 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_STRING_in_path_function994 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_34_in_path_function996 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_equality_value_function_in_value_function1022 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_compariable_value_function_in_value_function1026 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_equality_value_function1038 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_TRUE_in_equality_value_function1051 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_FALSE_in_equality_value_function1064 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NULL_in_equality_value_function1077 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_path_function_in_equality_value_function1090 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_NUMBER_in_compariable_value_function1102 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_time_millis_function_in_compariable_value_function1114 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_time_string_function_in_compariable_value_function1120 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_TIME_MILLIS_FUN_NAME_in_time_millis_function1132 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_33_in_time_millis_function1134 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_STRING_in_time_millis_function1136 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_time_millis_function1138 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_STRING_in_time_millis_function1140 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_34_in_time_millis_function1142 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_TIME_STRING_FUN_NAME_in_time_string_function1179 = new BitSet(new long[]{0x0000000200000000L});
    public static final BitSet FOLLOW_33_in_time_string_function1181 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_STRING_in_time_string_function1183 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_time_string_function1185 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_STRING_in_time_string_function1187 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_35_in_time_string_function1189 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_STRING_in_time_string_function1191 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_34_in_time_string_function1193 = new BitSet(new long[]{0x0000000000000002L});

}
