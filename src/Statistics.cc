#include "Statistics.h"


Statistics::Statistics()
{
	
}
Statistics::Statistics(Statistics &copyMe)
{
}
Statistics::~Statistics()
{
}
//unordered map
void Statistics::AddRel(char *relName, int numTuples)
{	
	relation.insert({relName, numTuples});
}
//map
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{	auto got = relation.find (relName);
	if (got == relation.end()) {
		cout<<"Relation not found"<<endl;
	} else {
		AttributeMap attr ;
		attr.insert({attName, numDistincts});
		
		auto got = relAttMap.find(relName);
		if (got != relAttMap.end()) {
			cout << "Attmap already exists" << endl;
			got->second.insert({attName, numDistincts});
		} else {
			relAttMap.insert({relName, attr});
		}
	}
}
void Statistics::CopyRel(char *oldName, char *newName)
{
	auto got = relation.find (oldName);
	if (got == relation.end()) {
		cout<<"Old Name not found; Cannot Replace"<<endl;
	} else {
		relation.insert({newName, got->second});
		AttributeMap attr ;
		
		auto got = relAttMap.find (oldName);
			attr.insert(got->second.begin(), got->second.end());
		relAttMap.insert({newName, attr});
	}
}
	
void Statistics::Read(const char *fromWhere)
{	string line;
	std::ifstream infile(fromWhere);
	
	while (std::getline(infile,line)){
		std::istringstream iss(line);
		
			vector<string> tokens;

				copy(istream_iterator<string>(iss),
			 istream_iterator<string>(),
			 back_inserter(tokens));
			 
			 string relName = tokens[0];
			 double tuples = stod(tokens[1], NULL);
			 AttributeMap attr ;
			 for (int i=2;i<tokens.size(); i+=2) {
				 attr.insert({tokens[i],stod(tokens[i+1], NULL) });
			 }
			 
			 relAttMap.insert({relName, attr});
			 relation.insert({relName, tuples});
			 
		}
	
}
void Statistics::Write(const char *fromWhere)
{
	std::ofstream outfile(fromWhere);
	for (auto ip: relAttMap) {
			outfile << ip.first <<" ";
			auto got = relation.find(ip.first);
			outfile << got->second <<" ";
			for (auto inmap : ip.second) {
				outfile << inmap.first <<" ";
				outfile << inmap.second <<" ";
			}
			outfile << endl;
		}
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
}
