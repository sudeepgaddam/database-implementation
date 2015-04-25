#include <iostream>
#include "ParseFunc.h"
#include "QueryPlanner.h"

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyfuncparse(void);
extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern	struct AndList *boolean; // the predicate in the WHERE clause
extern	struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern	struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern	int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern	int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query
using namespace std;

void fill_query(WholeQuery *query) {
	query->finalFunction = finalFunction;
	query->tables = tables;
	query->boolean = boolean;
	query->groupingAtts = groupingAtts;
	query->attsToSelect = attsToSelect;
	query->distinctAtts = distinctAtts;
	query->distinctFunc = distinctFunc;

}


int main () {
	
	yyfuncparse();
	WholeQuery query;
	fill_query(&query);
	QueryPlanner planQuery(query);
}
