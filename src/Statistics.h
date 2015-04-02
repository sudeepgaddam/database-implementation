#ifndef STATISTICS_H
#define STATISTICS_H

#include "ParseFunc.h"
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <utility>
#include <string>
#include <map>
#include <sstream>
#include <iterator>


//using std::unordered_map;
//using std::pair;
using namespace std;
typedef struct rel_attr {
	string relation_name;
	string attribute_name;
}rel_attr;
typedef std::pair<std::string, std::string> Att_key;

//http://stackoverflow.com/questions/7222143/unordered-map-hash-function-c  : to use pairs in unordered map
typedef std::unordered_map<std::string,double> AttributeMap;
typedef std::unordered_map<std::string,AttributeMap> RelAttMap;
typedef std::unordered_map<std::string, double> RelationMap;

class Statistics
{
private:
	RelationMap relation;
	RelAttMap relAttMap;
public:
	Statistics();
	Statistics(Statistics &copyMe);
	~Statistics();
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
