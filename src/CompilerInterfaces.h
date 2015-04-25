#ifndef COMPILERINTERFACES_H
#define	COMPILERINTERFACES_H
#include <map>
#include <vector>
#include "Compiler.h"
using namespace std;
class QueryPlanNode ;
class Parser
{
public:
    virtual bool ParseQuery()=0;
    virtual bool ErrorCheckQuery()=0;    
};

class Optimizer
{

public:
    virtual void GenerateQueryPlan()=0;
    virtual void DetermineJoinOrder()=0;
    virtual void SetQueryOps(bool *a,int s,QueryPlanNode **x)=0;
};


class QueryRunner
{
public:
    virtual void RunQuery()=0;
    virtual void setroot(QueryPlanNode *_root)=0;
};

#endif	/* COMPILERINTERFACES_H */

