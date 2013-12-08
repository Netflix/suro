// $ANTLR 3.4 MessageFilter.g 2012-08-22 11:55:59

package com.netflix.suro.routing.filter.lang;


import org.antlr.runtime.*;

@SuppressWarnings({"all", "warnings", "unchecked"})
public class MessageFilterLexer extends Lexer {
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


    // delegates
    // delegators
    public Lexer[] getDelegates() {
        return new Lexer[] {};
    }

    public MessageFilterLexer() {}
    public MessageFilterLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public MessageFilterLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);
    }
    public String getGrammarFileName() { return "MessageFilter.g"; }

    // $ANTLR start "AND"
    public final void mAND() throws RecognitionException {
        try {
            int _type = AND;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:33:5: ( 'and' )
            // MessageFilter.g:33:7: 'and'
            {
            match("and"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "AND"

    // $ANTLR start "BETWEEN"
    public final void mBETWEEN() throws RecognitionException {
        try {
            int _type = BETWEEN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:34:9: ( 'between' )
            // MessageFilter.g:34:11: 'between'
            {
            match("between"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "BETWEEN"

    // $ANTLR start "EQUALS"
    public final void mEQUALS() throws RecognitionException {
        try {
            int _type = EQUALS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:35:8: ( '=' )
            // MessageFilter.g:35:10: '='
            {
            match('='); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "EQUALS"

    // $ANTLR start "EXISTS"
    public final void mEXISTS() throws RecognitionException {
        try {
            int _type = EXISTS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:36:8: ( 'exists' )
            // MessageFilter.g:36:10: 'exists'
            {
            match("exists"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "EXISTS"

    // $ANTLR start "FALSE"
    public final void mFALSE() throws RecognitionException {
        try {
            int _type = FALSE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:37:7: ( 'false' )
            // MessageFilter.g:37:9: 'false'
            {
            match("false"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "FALSE"

    // $ANTLR start "GE"
    public final void mGE() throws RecognitionException {
        try {
            int _type = GE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:38:4: ( '>=' )
            // MessageFilter.g:38:6: '>='
            {
            match(">="); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "GE"

    // $ANTLR start "GT"
    public final void mGT() throws RecognitionException {
        try {
            int _type = GT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:39:4: ( '>' )
            // MessageFilter.g:39:6: '>'
            {
            match('>'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "GT"

    // $ANTLR start "IN"
    public final void mIN() throws RecognitionException {
        try {
            int _type = IN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:40:4: ( 'in' )
            // MessageFilter.g:40:6: 'in'
            {
            match("in"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "IN"

    // $ANTLR start "IS"
    public final void mIS() throws RecognitionException {
        try {
            int _type = IS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:41:4: ( 'is' )
            // MessageFilter.g:41:6: 'is'
            {
            match("is"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "IS"

    // $ANTLR start "LE"
    public final void mLE() throws RecognitionException {
        try {
            int _type = LE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:42:4: ( '<=' )
            // MessageFilter.g:42:6: '<='
            {
            match("<="); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "LE"

    // $ANTLR start "LT"
    public final void mLT() throws RecognitionException {
        try {
            int _type = LT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:43:4: ( '<' )
            // MessageFilter.g:43:6: '<'
            {
            match('<'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "LT"

    // $ANTLR start "MATCHES"
    public final void mMATCHES() throws RecognitionException {
        try {
            int _type = MATCHES;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:44:9: ( '=~' )
            // MessageFilter.g:44:11: '=~'
            {
            match("=~"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "MATCHES"

    // $ANTLR start "NOT"
    public final void mNOT() throws RecognitionException {
        try {
            int _type = NOT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:45:5: ( 'not' )
            // MessageFilter.g:45:7: 'not'
            {
            match("not"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "NOT"

    // $ANTLR start "NOT_EQUALS"
    public final void mNOT_EQUALS() throws RecognitionException {
        try {
            int _type = NOT_EQUALS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:46:12: ( '!=' )
            // MessageFilter.g:46:14: '!='
            {
            match("!="); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "NOT_EQUALS"

    // $ANTLR start "NULL"
    public final void mNULL() throws RecognitionException {
        try {
            int _type = NULL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:47:6: ( 'null' )
            // MessageFilter.g:47:8: 'null'
            {
            match("null"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "NULL"

    // $ANTLR start "OR"
    public final void mOR() throws RecognitionException {
        try {
            int _type = OR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:48:4: ( 'or' )
            // MessageFilter.g:48:6: 'or'
            {
            match("or"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "OR"

    // $ANTLR start "TIME_MILLIS_FUN_NAME"
    public final void mTIME_MILLIS_FUN_NAME() throws RecognitionException {
        try {
            int _type = TIME_MILLIS_FUN_NAME;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:49:22: ( 'time-millis' )
            // MessageFilter.g:49:24: 'time-millis'
            {
            match("time-millis"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "TIME_MILLIS_FUN_NAME"

    // $ANTLR start "TIME_STRING_FUN_NAME"
    public final void mTIME_STRING_FUN_NAME() throws RecognitionException {
        try {
            int _type = TIME_STRING_FUN_NAME;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:50:22: ( 'time-string' )
            // MessageFilter.g:50:24: 'time-string'
            {
            match("time-string"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "TIME_STRING_FUN_NAME"

    // $ANTLR start "TRUE"
    public final void mTRUE() throws RecognitionException {
        try {
            int _type = TRUE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:51:6: ( 'true' )
            // MessageFilter.g:51:8: 'true'
            {
            match("true"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "TRUE"

    // $ANTLR start "XPATH_FUN_NAME"
    public final void mXPATH_FUN_NAME() throws RecognitionException {
        try {
            int _type = XPATH_FUN_NAME;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:52:16: ( 'xpath' )
            // MessageFilter.g:52:18: 'xpath'
            {
            match("xpath"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "XPATH_FUN_NAME"

    // $ANTLR start "T__33"
    public final void mT__33() throws RecognitionException {
        try {
            int _type = T__33;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:53:7: ( '(' )
            // MessageFilter.g:53:9: '('
            {
            match('('); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__33"

    // $ANTLR start "T__34"
    public final void mT__34() throws RecognitionException {
        try {
            int _type = T__34;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:54:7: ( ')' )
            // MessageFilter.g:54:9: ')'
            {
            match(')'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__34"

    // $ANTLR start "T__35"
    public final void mT__35() throws RecognitionException {
        try {
            int _type = T__35;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:55:7: ( ',' )
            // MessageFilter.g:55:9: ','
            {
            match(','); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T__35"

    // $ANTLR start "NUMBER"
    public final void mNUMBER() throws RecognitionException {
        try {
            int _type = NUMBER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:198:5: ( ( '+' | '-' )? ( '0' .. '9' )+ ( '.' ( '0' .. '9' )* ( EXPONENT )? )? | ( '+' | '-' )? '.' ( '0' .. '9' )+ ( EXPONENT )? | ( '+' | '-' )? ( '0' .. '9' )+ EXPONENT )
            int alt11=3;
            alt11 = dfa11.predict(input);
            switch (alt11) {
                case 1 :
                    // MessageFilter.g:198:9: ( '+' | '-' )? ( '0' .. '9' )+ ( '.' ( '0' .. '9' )* ( EXPONENT )? )?
                    {
                    // MessageFilter.g:198:9: ( '+' | '-' )?
                    int alt1=2;
                    int LA1_0 = input.LA(1);

                    if ( (LA1_0=='+'||LA1_0=='-') ) {
                        alt1=1;
                    }
                    switch (alt1) {
                        case 1 :
                            // MessageFilter.g:
                            {
                            if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
                                input.consume();
                            }
                            else {
                                MismatchedSetException mse = new MismatchedSetException(null,input);
                                recover(mse);
                                throw mse;
                            }


                            }
                            break;

                    }


                    // MessageFilter.g:198:20: ( '0' .. '9' )+
                    int cnt2=0;
                    loop2:
                    do {
                        int alt2=2;
                        int LA2_0 = input.LA(1);

                        if ( ((LA2_0 >= '0' && LA2_0 <= '9')) ) {
                            alt2=1;
                        }


                        switch (alt2) {
                    	case 1 :
                    	    // MessageFilter.g:
                    	    {
                    	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                    	        input.consume();
                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    if ( cnt2 >= 1 ) break loop2;
                                EarlyExitException eee =
                                    new EarlyExitException(2, input);
                                throw eee;
                        }
                        cnt2++;
                    } while (true);


                    // MessageFilter.g:198:32: ( '.' ( '0' .. '9' )* ( EXPONENT )? )?
                    int alt5=2;
                    int LA5_0 = input.LA(1);

                    if ( (LA5_0=='.') ) {
                        alt5=1;
                    }
                    switch (alt5) {
                        case 1 :
                            // MessageFilter.g:198:33: '.' ( '0' .. '9' )* ( EXPONENT )?
                            {
                            match('.'); 

                            // MessageFilter.g:198:37: ( '0' .. '9' )*
                            loop3:
                            do {
                                int alt3=2;
                                int LA3_0 = input.LA(1);

                                if ( ((LA3_0 >= '0' && LA3_0 <= '9')) ) {
                                    alt3=1;
                                }


                                switch (alt3) {
                            	case 1 :
                            	    // MessageFilter.g:
                            	    {
                            	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                            	        input.consume();
                            	    }
                            	    else {
                            	        MismatchedSetException mse = new MismatchedSetException(null,input);
                            	        recover(mse);
                            	        throw mse;
                            	    }


                            	    }
                            	    break;

                            	default :
                            	    break loop3;
                                }
                            } while (true);


                            // MessageFilter.g:198:49: ( EXPONENT )?
                            int alt4=2;
                            int LA4_0 = input.LA(1);

                            if ( (LA4_0=='E'||LA4_0=='e') ) {
                                alt4=1;
                            }
                            switch (alt4) {
                                case 1 :
                                    // MessageFilter.g:198:49: EXPONENT
                                    {
                                    mEXPONENT(); 


                                    }
                                    break;

                            }


                            }
                            break;

                    }


                    }
                    break;
                case 2 :
                    // MessageFilter.g:199:9: ( '+' | '-' )? '.' ( '0' .. '9' )+ ( EXPONENT )?
                    {
                    // MessageFilter.g:199:9: ( '+' | '-' )?
                    int alt6=2;
                    int LA6_0 = input.LA(1);

                    if ( (LA6_0=='+'||LA6_0=='-') ) {
                        alt6=1;
                    }
                    switch (alt6) {
                        case 1 :
                            // MessageFilter.g:
                            {
                            if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
                                input.consume();
                            }
                            else {
                                MismatchedSetException mse = new MismatchedSetException(null,input);
                                recover(mse);
                                throw mse;
                            }


                            }
                            break;

                    }


                    match('.'); 

                    // MessageFilter.g:199:24: ( '0' .. '9' )+
                    int cnt7=0;
                    loop7:
                    do {
                        int alt7=2;
                        int LA7_0 = input.LA(1);

                        if ( ((LA7_0 >= '0' && LA7_0 <= '9')) ) {
                            alt7=1;
                        }


                        switch (alt7) {
                    	case 1 :
                    	    // MessageFilter.g:
                    	    {
                    	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                    	        input.consume();
                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    if ( cnt7 >= 1 ) break loop7;
                                EarlyExitException eee =
                                    new EarlyExitException(7, input);
                                throw eee;
                        }
                        cnt7++;
                    } while (true);


                    // MessageFilter.g:199:36: ( EXPONENT )?
                    int alt8=2;
                    int LA8_0 = input.LA(1);

                    if ( (LA8_0=='E'||LA8_0=='e') ) {
                        alt8=1;
                    }
                    switch (alt8) {
                        case 1 :
                            // MessageFilter.g:199:36: EXPONENT
                            {
                            mEXPONENT(); 


                            }
                            break;

                    }


                    }
                    break;
                case 3 :
                    // MessageFilter.g:200:9: ( '+' | '-' )? ( '0' .. '9' )+ EXPONENT
                    {
                    // MessageFilter.g:200:9: ( '+' | '-' )?
                    int alt9=2;
                    int LA9_0 = input.LA(1);

                    if ( (LA9_0=='+'||LA9_0=='-') ) {
                        alt9=1;
                    }
                    switch (alt9) {
                        case 1 :
                            // MessageFilter.g:
                            {
                            if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
                                input.consume();
                            }
                            else {
                                MismatchedSetException mse = new MismatchedSetException(null,input);
                                recover(mse);
                                throw mse;
                            }


                            }
                            break;

                    }


                    // MessageFilter.g:200:20: ( '0' .. '9' )+
                    int cnt10=0;
                    loop10:
                    do {
                        int alt10=2;
                        int LA10_0 = input.LA(1);

                        if ( ((LA10_0 >= '0' && LA10_0 <= '9')) ) {
                            alt10=1;
                        }


                        switch (alt10) {
                    	case 1 :
                    	    // MessageFilter.g:
                    	    {
                    	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                    	        input.consume();
                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    if ( cnt10 >= 1 ) break loop10;
                                EarlyExitException eee =
                                    new EarlyExitException(10, input);
                                throw eee;
                        }
                        cnt10++;
                    } while (true);


                    mEXPONENT(); 


                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "NUMBER"

    // $ANTLR start "COMMENT"
    public final void mCOMMENT() throws RecognitionException {
        try {
            int _type = COMMENT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:204:5: ( '//' (~ ( '\\n' | '\\r' ) )* ( '\\r' )? '\\n' | '/*' ( options {greedy=false; } : . )* '*/' )
            int alt15=2;
            int LA15_0 = input.LA(1);

            if ( (LA15_0=='/') ) {
                int LA15_1 = input.LA(2);

                if ( (LA15_1=='/') ) {
                    alt15=1;
                }
                else if ( (LA15_1=='*') ) {
                    alt15=2;
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 15, 1, input);

                    throw nvae;

                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 15, 0, input);

                throw nvae;

            }
            switch (alt15) {
                case 1 :
                    // MessageFilter.g:204:9: '//' (~ ( '\\n' | '\\r' ) )* ( '\\r' )? '\\n'
                    {
                    match("//"); 



                    // MessageFilter.g:204:14: (~ ( '\\n' | '\\r' ) )*
                    loop12:
                    do {
                        int alt12=2;
                        int LA12_0 = input.LA(1);

                        if ( ((LA12_0 >= '\u0000' && LA12_0 <= '\t')||(LA12_0 >= '\u000B' && LA12_0 <= '\f')||(LA12_0 >= '\u000E' && LA12_0 <= '\uFFFF')) ) {
                            alt12=1;
                        }


                        switch (alt12) {
                    	case 1 :
                    	    // MessageFilter.g:
                    	    {
                    	    if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '\t')||(input.LA(1) >= '\u000B' && input.LA(1) <= '\f')||(input.LA(1) >= '\u000E' && input.LA(1) <= '\uFFFF') ) {
                    	        input.consume();
                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop12;
                        }
                    } while (true);


                    // MessageFilter.g:204:28: ( '\\r' )?
                    int alt13=2;
                    int LA13_0 = input.LA(1);

                    if ( (LA13_0=='\r') ) {
                        alt13=1;
                    }
                    switch (alt13) {
                        case 1 :
                            // MessageFilter.g:204:28: '\\r'
                            {
                            match('\r'); 

                            }
                            break;

                    }


                    match('\n'); 

                    _channel=HIDDEN;

                    }
                    break;
                case 2 :
                    // MessageFilter.g:205:9: '/*' ( options {greedy=false; } : . )* '*/'
                    {
                    match("/*"); 



                    // MessageFilter.g:205:14: ( options {greedy=false; } : . )*
                    loop14:
                    do {
                        int alt14=2;
                        int LA14_0 = input.LA(1);

                        if ( (LA14_0=='*') ) {
                            int LA14_1 = input.LA(2);

                            if ( (LA14_1=='/') ) {
                                alt14=2;
                            }
                            else if ( ((LA14_1 >= '\u0000' && LA14_1 <= '.')||(LA14_1 >= '0' && LA14_1 <= '\uFFFF')) ) {
                                alt14=1;
                            }


                        }
                        else if ( ((LA14_0 >= '\u0000' && LA14_0 <= ')')||(LA14_0 >= '+' && LA14_0 <= '\uFFFF')) ) {
                            alt14=1;
                        }


                        switch (alt14) {
                    	case 1 :
                    	    // MessageFilter.g:205:42: .
                    	    {
                    	    matchAny(); 

                    	    }
                    	    break;

                    	default :
                    	    break loop14;
                        }
                    } while (true);


                    match("*/"); 



                    _channel=HIDDEN;

                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "COMMENT"

    // $ANTLR start "WS"
    public final void mWS() throws RecognitionException {
        try {
            int _type = WS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:208:5: ( ( ' ' | '\\t' | '\\r' | '\\n' ) )
            // MessageFilter.g:208:9: ( ' ' | '\\t' | '\\r' | '\\n' )
            {
            if ( (input.LA(1) >= '\t' && input.LA(1) <= '\n')||input.LA(1)=='\r'||input.LA(1)==' ' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            _channel=HIDDEN;

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "WS"

    // $ANTLR start "STRING"
    public final void mSTRING() throws RecognitionException {
        try {
            int _type = STRING;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // MessageFilter.g:216:5: ( '\"' ( ESC_SEQ |~ ( '\\\\' | '\"' ) )* '\"' )
            // MessageFilter.g:216:8: '\"' ( ESC_SEQ |~ ( '\\\\' | '\"' ) )* '\"'
            {
            match('\"'); 

            // MessageFilter.g:216:12: ( ESC_SEQ |~ ( '\\\\' | '\"' ) )*
            loop16:
            do {
                int alt16=3;
                int LA16_0 = input.LA(1);

                if ( (LA16_0=='\\') ) {
                    alt16=1;
                }
                else if ( ((LA16_0 >= '\u0000' && LA16_0 <= '!')||(LA16_0 >= '#' && LA16_0 <= '[')||(LA16_0 >= ']' && LA16_0 <= '\uFFFF')) ) {
                    alt16=2;
                }


                switch (alt16) {
            	case 1 :
            	    // MessageFilter.g:216:14: ESC_SEQ
            	    {
            	    mESC_SEQ(); 


            	    }
            	    break;
            	case 2 :
            	    // MessageFilter.g:216:24: ~ ( '\\\\' | '\"' )
            	    {
            	    if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '!')||(input.LA(1) >= '#' && input.LA(1) <= '[')||(input.LA(1) >= ']' && input.LA(1) <= '\uFFFF') ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    break loop16;
                }
            } while (true);


            match('\"'); 

            setText(getText().substring(1, getText().length()-1));

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "STRING"

    // $ANTLR start "HEX_DIGIT"
    public final void mHEX_DIGIT() throws RecognitionException {
        try {
            // MessageFilter.g:221:11: ( ( '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' ) )
            // MessageFilter.g:
            {
            if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'F')||(input.LA(1) >= 'a' && input.LA(1) <= 'f') ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "HEX_DIGIT"

    // $ANTLR start "ESC_SEQ"
    public final void mESC_SEQ() throws RecognitionException {
        try {
            // MessageFilter.g:225:5: ( '\\\\' ( 'b' | 't' | 'n' | 'f' | 'r' | '\\\"' | '\\'' | '\\\\' ) | UNICODE_ESC | OCTAL_ESC )
            int alt17=3;
            int LA17_0 = input.LA(1);

            if ( (LA17_0=='\\') ) {
                switch ( input.LA(2) ) {
                case '\"':
                case '\'':
                case '\\':
                case 'b':
                case 'f':
                case 'n':
                case 'r':
                case 't':
                    {
                    alt17=1;
                    }
                    break;
                case 'u':
                    {
                    alt17=2;
                    }
                    break;
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                    {
                    alt17=3;
                    }
                    break;
                default:
                    NoViableAltException nvae =
                        new NoViableAltException("", 17, 1, input);

                    throw nvae;

                }

            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 17, 0, input);

                throw nvae;

            }
            switch (alt17) {
                case 1 :
                    // MessageFilter.g:225:9: '\\\\' ( 'b' | 't' | 'n' | 'f' | 'r' | '\\\"' | '\\'' | '\\\\' )
                    {
                    match('\\'); 

                    if ( input.LA(1)=='\"'||input.LA(1)=='\''||input.LA(1)=='\\'||input.LA(1)=='b'||input.LA(1)=='f'||input.LA(1)=='n'||input.LA(1)=='r'||input.LA(1)=='t' ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    }
                    break;
                case 2 :
                    // MessageFilter.g:226:9: UNICODE_ESC
                    {
                    mUNICODE_ESC(); 


                    }
                    break;
                case 3 :
                    // MessageFilter.g:227:9: OCTAL_ESC
                    {
                    mOCTAL_ESC(); 


                    }
                    break;

            }

        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "ESC_SEQ"

    // $ANTLR start "OCTAL_ESC"
    public final void mOCTAL_ESC() throws RecognitionException {
        try {
            // MessageFilter.g:232:5: ( '\\\\' ( '0' .. '3' ) ( '0' .. '7' ) ( '0' .. '7' ) | '\\\\' ( '0' .. '7' ) ( '0' .. '7' ) | '\\\\' ( '0' .. '7' ) )
            int alt18=3;
            int LA18_0 = input.LA(1);

            if ( (LA18_0=='\\') ) {
                int LA18_1 = input.LA(2);

                if ( ((LA18_1 >= '0' && LA18_1 <= '3')) ) {
                    int LA18_2 = input.LA(3);

                    if ( ((LA18_2 >= '0' && LA18_2 <= '7')) ) {
                        int LA18_4 = input.LA(4);

                        if ( ((LA18_4 >= '0' && LA18_4 <= '7')) ) {
                            alt18=1;
                        }
                        else {
                            alt18=2;
                        }
                    }
                    else {
                        alt18=3;
                    }
                }
                else if ( ((LA18_1 >= '4' && LA18_1 <= '7')) ) {
                    int LA18_3 = input.LA(3);

                    if ( ((LA18_3 >= '0' && LA18_3 <= '7')) ) {
                        alt18=2;
                    }
                    else {
                        alt18=3;
                    }
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 18, 1, input);

                    throw nvae;

                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 18, 0, input);

                throw nvae;

            }
            switch (alt18) {
                case 1 :
                    // MessageFilter.g:232:9: '\\\\' ( '0' .. '3' ) ( '0' .. '7' ) ( '0' .. '7' )
                    {
                    match('\\'); 

                    if ( (input.LA(1) >= '0' && input.LA(1) <= '3') ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    if ( (input.LA(1) >= '0' && input.LA(1) <= '7') ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    if ( (input.LA(1) >= '0' && input.LA(1) <= '7') ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    }
                    break;
                case 2 :
                    // MessageFilter.g:233:9: '\\\\' ( '0' .. '7' ) ( '0' .. '7' )
                    {
                    match('\\'); 

                    if ( (input.LA(1) >= '0' && input.LA(1) <= '7') ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    if ( (input.LA(1) >= '0' && input.LA(1) <= '7') ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    }
                    break;
                case 3 :
                    // MessageFilter.g:234:9: '\\\\' ( '0' .. '7' )
                    {
                    match('\\'); 

                    if ( (input.LA(1) >= '0' && input.LA(1) <= '7') ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    }
                    break;

            }

        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "OCTAL_ESC"

    // $ANTLR start "UNICODE_ESC"
    public final void mUNICODE_ESC() throws RecognitionException {
        try {
            // MessageFilter.g:239:5: ( '\\\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT )
            // MessageFilter.g:239:9: '\\\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
            {
            match('\\'); 

            match('u'); 

            mHEX_DIGIT(); 


            mHEX_DIGIT(); 


            mHEX_DIGIT(); 


            mHEX_DIGIT(); 


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "UNICODE_ESC"

    // $ANTLR start "EXPONENT"
    public final void mEXPONENT() throws RecognitionException {
        try {
            // MessageFilter.g:243:10: ( ( 'e' | 'E' ) ( '+' | '-' )? ( '0' .. '9' )+ )
            // MessageFilter.g:243:12: ( 'e' | 'E' ) ( '+' | '-' )? ( '0' .. '9' )+
            {
            if ( input.LA(1)=='E'||input.LA(1)=='e' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            // MessageFilter.g:243:22: ( '+' | '-' )?
            int alt19=2;
            int LA19_0 = input.LA(1);

            if ( (LA19_0=='+'||LA19_0=='-') ) {
                alt19=1;
            }
            switch (alt19) {
                case 1 :
                    // MessageFilter.g:
                    {
                    if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    }
                    break;

            }


            // MessageFilter.g:243:33: ( '0' .. '9' )+
            int cnt20=0;
            loop20:
            do {
                int alt20=2;
                int LA20_0 = input.LA(1);

                if ( ((LA20_0 >= '0' && LA20_0 <= '9')) ) {
                    alt20=1;
                }


                switch (alt20) {
            	case 1 :
            	    // MessageFilter.g:
            	    {
            	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    if ( cnt20 >= 1 ) break loop20;
                        EarlyExitException eee =
                            new EarlyExitException(20, input);
                        throw eee;
                }
                cnt20++;
            } while (true);


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "EXPONENT"

    public void mTokens() throws RecognitionException {
        // MessageFilter.g:1:8: ( AND | BETWEEN | EQUALS | EXISTS | FALSE | GE | GT | IN | IS | LE | LT | MATCHES | NOT | NOT_EQUALS | NULL | OR | TIME_MILLIS_FUN_NAME | TIME_STRING_FUN_NAME | TRUE | XPATH_FUN_NAME | T__33 | T__34 | T__35 | NUMBER | COMMENT | WS | STRING )
        int alt21=27;
        switch ( input.LA(1) ) {
        case 'a':
            {
            alt21=1;
            }
            break;
        case 'b':
            {
            alt21=2;
            }
            break;
        case '=':
            {
            int LA21_3 = input.LA(2);

            if ( (LA21_3=='~') ) {
                alt21=12;
            }
            else {
                alt21=3;
            }
            }
            break;
        case 'e':
            {
            alt21=4;
            }
            break;
        case 'f':
            {
            alt21=5;
            }
            break;
        case '>':
            {
            int LA21_6 = input.LA(2);

            if ( (LA21_6=='=') ) {
                alt21=6;
            }
            else {
                alt21=7;
            }
            }
            break;
        case 'i':
            {
            int LA21_7 = input.LA(2);

            if ( (LA21_7=='n') ) {
                alt21=8;
            }
            else if ( (LA21_7=='s') ) {
                alt21=9;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 21, 7, input);

                throw nvae;

            }
            }
            break;
        case '<':
            {
            int LA21_8 = input.LA(2);

            if ( (LA21_8=='=') ) {
                alt21=10;
            }
            else {
                alt21=11;
            }
            }
            break;
        case 'n':
            {
            int LA21_9 = input.LA(2);

            if ( (LA21_9=='o') ) {
                alt21=13;
            }
            else if ( (LA21_9=='u') ) {
                alt21=15;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 21, 9, input);

                throw nvae;

            }
            }
            break;
        case '!':
            {
            alt21=14;
            }
            break;
        case 'o':
            {
            alt21=16;
            }
            break;
        case 't':
            {
            int LA21_12 = input.LA(2);

            if ( (LA21_12=='i') ) {
                int LA21_31 = input.LA(3);

                if ( (LA21_31=='m') ) {
                    int LA21_33 = input.LA(4);

                    if ( (LA21_33=='e') ) {
                        int LA21_34 = input.LA(5);

                        if ( (LA21_34=='-') ) {
                            int LA21_35 = input.LA(6);

                            if ( (LA21_35=='m') ) {
                                alt21=17;
                            }
                            else if ( (LA21_35=='s') ) {
                                alt21=18;
                            }
                            else {
                                NoViableAltException nvae =
                                    new NoViableAltException("", 21, 35, input);

                                throw nvae;

                            }
                        }
                        else {
                            NoViableAltException nvae =
                                new NoViableAltException("", 21, 34, input);

                            throw nvae;

                        }
                    }
                    else {
                        NoViableAltException nvae =
                            new NoViableAltException("", 21, 33, input);

                        throw nvae;

                    }
                }
                else {
                    NoViableAltException nvae =
                        new NoViableAltException("", 21, 31, input);

                    throw nvae;

                }
            }
            else if ( (LA21_12=='r') ) {
                alt21=19;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 21, 12, input);

                throw nvae;

            }
            }
            break;
        case 'x':
            {
            alt21=20;
            }
            break;
        case '(':
            {
            alt21=21;
            }
            break;
        case ')':
            {
            alt21=22;
            }
            break;
        case ',':
            {
            alt21=23;
            }
            break;
        case '+':
        case '-':
        case '.':
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            {
            alt21=24;
            }
            break;
        case '/':
            {
            alt21=25;
            }
            break;
        case '\t':
        case '\n':
        case '\r':
        case ' ':
            {
            alt21=26;
            }
            break;
        case '\"':
            {
            alt21=27;
            }
            break;
        default:
            NoViableAltException nvae =
                new NoViableAltException("", 21, 0, input);

            throw nvae;

        }

        switch (alt21) {
            case 1 :
                // MessageFilter.g:1:10: AND
                {
                mAND(); 


                }
                break;
            case 2 :
                // MessageFilter.g:1:14: BETWEEN
                {
                mBETWEEN(); 


                }
                break;
            case 3 :
                // MessageFilter.g:1:22: EQUALS
                {
                mEQUALS(); 


                }
                break;
            case 4 :
                // MessageFilter.g:1:29: EXISTS
                {
                mEXISTS(); 


                }
                break;
            case 5 :
                // MessageFilter.g:1:36: FALSE
                {
                mFALSE(); 


                }
                break;
            case 6 :
                // MessageFilter.g:1:42: GE
                {
                mGE(); 


                }
                break;
            case 7 :
                // MessageFilter.g:1:45: GT
                {
                mGT(); 


                }
                break;
            case 8 :
                // MessageFilter.g:1:48: IN
                {
                mIN(); 


                }
                break;
            case 9 :
                // MessageFilter.g:1:51: IS
                {
                mIS(); 


                }
                break;
            case 10 :
                // MessageFilter.g:1:54: LE
                {
                mLE(); 


                }
                break;
            case 11 :
                // MessageFilter.g:1:57: LT
                {
                mLT(); 


                }
                break;
            case 12 :
                // MessageFilter.g:1:60: MATCHES
                {
                mMATCHES(); 


                }
                break;
            case 13 :
                // MessageFilter.g:1:68: NOT
                {
                mNOT(); 


                }
                break;
            case 14 :
                // MessageFilter.g:1:72: NOT_EQUALS
                {
                mNOT_EQUALS(); 


                }
                break;
            case 15 :
                // MessageFilter.g:1:83: NULL
                {
                mNULL(); 


                }
                break;
            case 16 :
                // MessageFilter.g:1:88: OR
                {
                mOR(); 


                }
                break;
            case 17 :
                // MessageFilter.g:1:91: TIME_MILLIS_FUN_NAME
                {
                mTIME_MILLIS_FUN_NAME(); 


                }
                break;
            case 18 :
                // MessageFilter.g:1:112: TIME_STRING_FUN_NAME
                {
                mTIME_STRING_FUN_NAME(); 


                }
                break;
            case 19 :
                // MessageFilter.g:1:133: TRUE
                {
                mTRUE(); 


                }
                break;
            case 20 :
                // MessageFilter.g:1:138: XPATH_FUN_NAME
                {
                mXPATH_FUN_NAME(); 


                }
                break;
            case 21 :
                // MessageFilter.g:1:153: T__33
                {
                mT__33(); 


                }
                break;
            case 22 :
                // MessageFilter.g:1:159: T__34
                {
                mT__34(); 


                }
                break;
            case 23 :
                // MessageFilter.g:1:165: T__35
                {
                mT__35(); 


                }
                break;
            case 24 :
                // MessageFilter.g:1:171: NUMBER
                {
                mNUMBER(); 


                }
                break;
            case 25 :
                // MessageFilter.g:1:178: COMMENT
                {
                mCOMMENT(); 


                }
                break;
            case 26 :
                // MessageFilter.g:1:186: WS
                {
                mWS(); 


                }
                break;
            case 27 :
                // MessageFilter.g:1:189: STRING
                {
                mSTRING(); 


                }
                break;

        }

    }


    protected DFA11 dfa11 = new DFA11(this);
    static final String DFA11_eotS =
        "\2\uffff\1\4\3\uffff";
    static final String DFA11_eofS =
        "\6\uffff";
    static final String DFA11_minS =
        "\1\53\1\56\1\60\3\uffff";
    static final String DFA11_maxS =
        "\2\71\1\145\3\uffff";
    static final String DFA11_acceptS =
        "\3\uffff\1\2\1\1\1\3";
    static final String DFA11_specialS =
        "\6\uffff}>";
    static final String[] DFA11_transitionS = {
            "\1\1\1\uffff\1\1\1\3\1\uffff\12\2",
            "\1\3\1\uffff\12\2",
            "\12\2\13\uffff\1\5\37\uffff\1\5",
            "",
            "",
            ""
    };

    static final short[] DFA11_eot = DFA.unpackEncodedString(DFA11_eotS);
    static final short[] DFA11_eof = DFA.unpackEncodedString(DFA11_eofS);
    static final char[] DFA11_min = DFA.unpackEncodedStringToUnsignedChars(DFA11_minS);
    static final char[] DFA11_max = DFA.unpackEncodedStringToUnsignedChars(DFA11_maxS);
    static final short[] DFA11_accept = DFA.unpackEncodedString(DFA11_acceptS);
    static final short[] DFA11_special = DFA.unpackEncodedString(DFA11_specialS);
    static final short[][] DFA11_transition;

    static {
        int numStates = DFA11_transitionS.length;
        DFA11_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA11_transition[i] = DFA.unpackEncodedString(DFA11_transitionS[i]);
        }
    }

    class DFA11 extends DFA {

        public DFA11(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 11;
            this.eot = DFA11_eot;
            this.eof = DFA11_eof;
            this.min = DFA11_min;
            this.max = DFA11_max;
            this.accept = DFA11_accept;
            this.special = DFA11_special;
            this.transition = DFA11_transition;
        }
        public String getDescription() {
            return "197:1: NUMBER : ( ( '+' | '-' )? ( '0' .. '9' )+ ( '.' ( '0' .. '9' )* ( EXPONENT )? )? | ( '+' | '-' )? '.' ( '0' .. '9' )+ ( EXPONENT )? | ( '+' | '-' )? ( '0' .. '9' )+ EXPONENT );";
        }
    }
 

}
