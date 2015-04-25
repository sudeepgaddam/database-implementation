#ifndef QUERYPLANNER_H
#define QUERYPLANNER_H

#include "ParseFunc.h"
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <string>
#include <vector>
#include <memory>
#include <algorithm>
#include <sstream>
#include <iterator>

typedef struct WholeQuery {

    struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
	struct TableList *tables; // the list of tables and aliases in the query
	struct AndList *boolean; // the predicate in the WHERE clause
	struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
	struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
	int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
	int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

}WholeQuery;


class QueryPlanner
{
private:
	
public:
	QueryPlanner(WholeQuery query);
	QueryPlanner();
	~QueryPlanner();
	void BuildQueryPlan();
	void PrintQueryPlan ();
	void ExecuteQueryPlan();
};

#endif
