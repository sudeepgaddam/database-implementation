/***********************************************************************
 * SECTION 1
 ***********************************************************************/
/* The code in %{ %} is included as it is in lex.yy.c file
 * it has C global variables, prototypes, and comments 
 */


%{

#include <string.h> // For strdup()
#include <stdlib.h> // For malloc()
#include "src/ParseFunc.h"
#include "yyfunc.tab.h"

//int yyfunclineno = 1;
void yyfuncerror(char*s);

static YY_BUFFER_STATE yyfunc_buf_state;
void init_lexical_parser_func (char *src) { yyfunc_buf_state = yyfunc_scan_string (src); }
void close_lexical_parser_func () { yyfunc_delete_buffer (yyfunc_buf_state); }

%}


/******************************************************************************
 * SECTION 2
 ******************************************************************************/
/* This is the DEFINITION section which contains substitutions, code, and
 * start stats; will be copied into lex.yy.c
 */

/******************************************************************************
 * SECTION 3
 ******************************************************************************/
/* This is the RULES section which defines how to "scan" and what action
 * to take for each token
 */

%%

"SELECT"		return(SELECT);

"FROM"			return(FROM);

"WHERE"			return(WHERE);

"SUM"			return(SUM);

"AND"			return(AND);

"GROUP"			return(GROUP);

"DISTINCT"		return(DISTINCT);

"BY"			return(BY);

"OR"			return(OR);

"AS"			return(AS);

"("			return('(');

"<"                     return('<');

">"                     return('>');

"="                     return('=');

")"    	        	return(')');

"+"    	        	return('+');

"-"    	        	return('-');

"/"    	        	return('/');

"*"    	        	return('*');

","    	        	return(',');

-?[0-9]+ 	       {yyfunclval.actualChars = strdup(yyfunctext);
  			return(Int); 
		        }

-?[0-9]+\.[0-9]*       {yyfunclval.actualChars = strdup(yyfunctext); 
  			return(Float);
			}

[A-Za-z][A-Za-z0-9_-]* {yyfunclval.actualChars = strdup(yyfunctext);
  			return(Name);
			}     

[A-Za-z][A-Za-z0-9_-]*\.[A-Za-z][A-Za-z0-9_-]* {yyfunclval.actualChars = strdup(yyfunctext);
  			return(Name);
			}     

\'[^'\n]*\'            {/* take care of ' in a string */
                        if (yyfunctext[yyleng - 2] == '\\') {
                                yymore();
                        } else {
                                yyfunclval.actualChars = strdup(yyfunctext + 1);
                                yyfunclval.actualChars[strlen(yyfunclval.actualChars) - 1] = 0;
                                return(String);
                        }
                        }

\n                	yyfunclineno++;

[ \t]             	;

.                 	yyfuncerror("LEX_ERROR: invalid character");

%%

void yyfuncerror(char *s) {
  printf("%d: %s at %s\n", yyfunclineno, s, yyfunctext);
}

int yyfuncwrap(void){
  return 1;
}
