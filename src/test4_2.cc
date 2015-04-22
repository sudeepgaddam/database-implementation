
#include <iostream>
#include "ParseFunc.h"
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;

using namespace std;
using namespace std;


int main () {

	yyparse();
}



