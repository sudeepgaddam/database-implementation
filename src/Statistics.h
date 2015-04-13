#ifndef STATISTICS_H
#define STATISTICS_H

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


//using std::unordered_map;
//using std::pair;
using namespace std;

typedef struct Partition{

	int partitionNum;
	std::unordered_map<std::string,unsigned long long int> AttributeMap;
	unsigned long long int numTuples;
	std::vector<string> relations;
}Partition;

typedef struct AttInfo{
	int partitionNum;
	unsigned long long int numTuples; //in that partition
	unsigned long long int numDistinct;
}AttInfo;

class Statistics
{
private:
	std::unordered_map<int, Partition> partitionsMap;
	std::unordered_map<std::string, int> relationToPartitionMap;
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

	bool checkRelNames(char **relNames, int numToJoin);
	vector<std::string> getSet(string relation);
	AttInfo getAttInfo(string attr);
	double getCNFSelectivity(string attName, vector<string> &orAttributes, double selFac, int oper);
	std::pair<std::pair<unsigned long long int, double>, std::pair<int,int>> JoinCost(const struct AndList *andList);
	void mergePartitions(Partition& p1, Partition& p2, int newTuples);
	Partition& getPartition(int partNum);
};

#endif
