#ifndef STATISTICS_H
#define STATISTICS_H

#include "ParseTree.h"
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <utility>
#include <string>

using std::unordered_map;
using std::pair;
using namespace std;

//typedef std::pair<std::string, std::string> Att_key;
//typedef std::unordered_map<Att_key,double> Attribute;
//typedef std::unordered_map<std::string, double> Relation;

class Statistics
{
private:
	//Relation relation;
	//Attribute attribute;
public:
	Statistics();
	Statistics(Statistics &copyMe);
	//Uses UO Maps
	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	//Read write Functions
	void Read(const char *fromWhere);
	void Write(const char *fromWhere);
	
	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

};

#endif
