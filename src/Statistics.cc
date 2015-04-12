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
	//create new PartitionObj for relName
	Partition p;
	p.partitionNum = partitionsMap.size() + 1;
	p.numTuples = numTuples;
	p.relations.push_back(relName);

	//update data structs
	partitionsMap.insert(std::make_pair(p.partitionNum, p));
	relationToPartitionMap.insert({relName, p.partitionNum});
}
//map
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{	
	auto got = relationToPartitionMap.find(relName);
	if (got == relationToPartitionMap.end()){
		cout<<"RelName not present in DB" << endl;
	}else{
		int partitionNum = got->second;
		auto got = partitionsMap.find(partitionNum);
		if (got == partitionsMap.end()){
			cout << "PartitionObject was not created for this relName"<<endl;
		}else{
			Partition &p = got->second;						
			p.AttributeMap.insert({attName, numDistincts});			
		}
	}
}

//copy if relname is present as independent partition
void Statistics::CopyRel(char *oldName, char *newName)
{
	auto got = relationToPartitionMap.find (oldName);
	if (got == relationToPartitionMap.end()) {
		cout<<"Old Name not found; Cannot Replace"<<endl;
	} else {
		int newPartitionNum = partitionsMap.size()+1;
		relationToPartitionMap.insert({newName, newPartitionNum});
		int oldPartitionNum = got->second;
		auto got = partitionsMap.find(oldPartitionNum);
		if(got == partitionsMap.end()){
			cout<<"Not able to find PartitionObject for oldName"<<endl;
		}else{
			Partition &p = got->second;
			Partition newp;
			newp.partitionNum = newPartitionNum;
			newp.numTuples = p.numTuples;
			//copy AttributeMap
			newp.AttributeMap = p.AttributeMap;
			vector<string> rels = p.relations;
			for (string s: rels){
				newp.relations.push_back(s);
			}
			partitionsMap.insert(std::make_pair(newPartitionNum, newp));
		}
	}
}
	
void Statistics::Read(const char *fromWhere)
{	string line;
	std::ifstream infile(fromWhere);

	std::getline(infile,line);
	int numPartitions = stoi(line);

	
	std::getline(infile,line);
	std::istringstream iss(line);
	vector<string> tokens;
	copy(istream_iterator<string>(iss),
	istream_iterator<string>(),
	back_inserter(tokens));
	for (int i=0;i<tokens.size(); i+=2) {
		relationToPartitionMap.insert({tokens[i],stoi(tokens[i+1], NULL) });
	}

	while (std::getline(infile,line)){
		std::istringstream iss(line);
		vector<string> tokens;
		copy(istream_iterator<string>(iss),
		istream_iterator<string>(),
		back_inserter(tokens));
		
		int partitionNumber = stoi(tokens[0], NULL);
		int numTuples = stoi(tokens[1], NULL);

		vector<std::string> relations;
		int numRels = stoi(tokens[2], NULL);
		for (int i=1; i<= numRels; i++){
			relations.push_back(tokens[2+i]);
		}

		std::unordered_map<std::string,int> attr;
		for (int i=3 + numRels;i<tokens.size(); i+=2) {
			attr.insert({tokens[i],stoi(tokens[i+1], NULL) });
		}
		Partition p;
		p.partitionNum = partitionNumber;
		p.numTuples = numTuples;
		p.AttributeMap = attr;
		p.relations = relations;
		partitionsMap.insert(std::make_pair(partitionNumber, p));
			 
	}
	
}
void Statistics::Write(const char *fromWhere)
{
	std::ofstream outfile(fromWhere);
	outfile << relationToPartitionMap.size() << endl;
	for (auto ip: relationToPartitionMap) {
		outfile << ip.first <<" ";
		auto got = relationToPartitionMap.find(ip.first);
		outfile << got->second <<" ";
	}
	outfile << endl;
	for (auto ip: partitionsMap){
		outfile << ip.first << " ";
		auto got = partitionsMap.find(ip.first);
		outfile << got->second.numTuples << " ";

		int numRels = got->second.relations.size();
		outfile << numRels << " ";
	
		for (int i=0; i<numRels; i++){
			outfile << got->second.relations[i] << " ";
		}

		for (auto att: got->second.AttributeMap){
			outfile << att.first << " " << att.second << " ";
		}
		outfile << endl;
	}
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
	relationToPartitionMap.insert({"partsupp", 1});

	bool valid = checkRelNames(relNames, numToJoin);
	if (!valid) {cout << "Given RelName not found in DB" << endl; return -1;}

	
	
}

bool Statistics::checkRelNames(char **relNames, int numToJoin){
	std::vector<std::string> v(relNames, relNames + numToJoin);
	for (auto rel : v){
		vector<std::string> relsetvec = getSet(rel);
		if (relsetvec.size()==0) return false;
		for (string setRel: relsetvec){
			bool found = false;			
			for (auto ip: v){
				if(ip.compare(setRel)==0){
					found = true;
				}
			}
			if (!found)
				return false;
		}
	}
	return true;
}

vector<std::string> Statistics::getSet(string relation){
	vector<std::string> setvec;
	auto got = relationToPartitionMap.find(relation);
	if (got == relationToPartitionMap.end()){
		return setvec;
	}
	int partitionNum = got->second;
	for (auto ip: relationToPartitionMap){
		if(ip.second == partitionNum){
			setvec.push_back(ip.first);
		}
	}
	return setvec;
}
