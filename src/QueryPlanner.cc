#include "QueryPlanner.h"
using namespace std;

void PrintOperand(struct Operand *pOperand)
{
        if(pOperand!=NULL)
        {
                cout<<pOperand->value<<" ";
        }
        else
                return;
}

void PrintComparisonOp(struct ComparisonOp *pCom)
{
        if(pCom!=NULL)
        {
                PrintOperand(pCom->left);
                switch(pCom->code)
                {
                        case 1:
                                cout<<" < "; break;
                        case 2:
                                cout<<" > "; break;
                        case 3:
                                cout<<" = ";
                }
                PrintOperand(pCom->right);

        }
        else
        {
                return;
        }
}
void PrintOrList(struct OrList *pOr)
{
        if(pOr !=NULL)
        {
                struct ComparisonOp *pCom = pOr->left;
                PrintComparisonOp(pCom);

                if(pOr->rightOr)
                {
                        cout<<" OR ";
                        PrintOrList(pOr->rightOr);
                }
        }
        else
        {
                return;
        }
}
void PrintAndList(struct AndList *pAnd)
{		
        if(pAnd !=NULL)
        {
                struct OrList *pOr = pAnd->left;
                PrintOrList(pOr);
                if(pAnd->rightAnd)
                {
                        cout<<" AND ";
                        PrintAndList(pAnd->rightAnd);
                }
        }
        else
        {		cout<<endl;
                return;
        }
}
void PrintNamesList (struct NameList *names)
{		
        if(names !=NULL)
        {	printf("%s ", names->name);
            if(names->next) {
               cout<<", ";
               PrintNamesList(names->next);
                }
        }
        else
        {		cout<<endl;
                return;
        }
}

void PrintTableList (struct TableList *tables)
{		
        if(tables !=NULL)
        {	printf(" Actual Table Name : %s, Alias Table Name : %s \n", tables->tableName, tables->aliasAs);
            if(tables->next) {
               cout<<", ";
               PrintTableList(tables->next);
                }
        }
        else
        {		cout<<endl;
                return;
        }
}
QueryPlanner::QueryPlanner(WholeQuery query)
{
	cout << "Printing Where And list:";
	PrintAndList(query.boolean);
	cout <<endl << "Printing Grouping atributes:";
	PrintNamesList(query.groupingAtts);
		cout << endl << "Printing Selection Names list:";
	PrintNamesList(query.attsToSelect);
	cout << endl << "Printing Tables list:";
	PrintTableList(query.tables);
	cout << endl;
}

QueryPlanner::~QueryPlanner()
{
	
}

QueryPlanner::QueryPlanner()
{
	
}

void QueryPlanner :: BuildQueryPlan()
{
	
}

void QueryPlanner :: PrintQueryPlan()
{
}

void QueryPlanner :: ExecuteQueryPlan()
{
}
