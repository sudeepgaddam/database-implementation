#include "Statistics.h"


void Statistics :: AddRel(char *relName, int numTuples) {
	//relations.Insert({relName, numTuples});
}

void Statistics :: AddAtt(char *relName, char *attName, int numDistincts) {
}

void Statistics :: CopyRel(char *oldName, char *newName){
}

void Statistics :: Read(const char *fromWhere){
	
}

void Statistics :: Write(const char *fromWhere) {
}

void Statistics ::  Apply(struct AndList *parseTree, char *relNames[], int numToJoin) {

}
double Statistics ::  Estimate(struct AndList *parseTree, char **relNames, int numToJoin){

}
