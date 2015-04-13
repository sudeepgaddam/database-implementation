#include "Statistics.h"
using namespace std;

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
		unsigned long long int numTuples = stoi(tokens[1], NULL);
		std::unordered_map<std::string,unsigned long long int> attr;
		for (int i=2;i<tokens.size(); i+=2) {
			attr.insert({tokens[i],stoi(tokens[i+1], NULL) });
		}
		Partition p;
		p.partitionNum = partitionNumber;
		p.numTuples = numTuples;
		p.AttributeMap = attr;
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
		for (auto att: got->second.AttributeMap){
			outfile << att.first << " " << att.second << " ";
		}
		outfile << endl;
	}
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
	double estimate = Estimate(parseTree, relNames, numToJoin);
	if (estimate == -1){
		cout << "cannot Apply - check input" << endl;		
		return;
	}
	
	/*int part1;
	int part2;

	Partition p1;
	Partition p2;*/

	//mergePartitions(p1, p2, estimate);

	
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{

	bool valid = checkRelNames(relNames, numToJoin);
	if (!valid) {cout << "Given RelName not found in DB" << endl; return -1;}

	return JoinCost(parseTree);
	
}

bool Statistics::checkRelNames(char **relNames, int numToJoin){
	std::vector<std::string> v(relNames, relNames + numToJoin);
	for (auto rel : v){
		vector<std::string> relsetvec = getSet(rel);
		if (relsetvec.size()==0) return false;
		//cout << "found v.size(): " << v.size() << " relSet.size(): " << relsetvec.size() << endl;
		for (string setRel: relsetvec){
			bool found = false;			
			for (auto ip: v){
				//cout << "setRel: " << setRel << " v.rel: " << ip << endl;
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

// returns relation.numOfTuples, numOfDistinctValues;
// returns partitionNumber of attr, numOfDistinctValues;
AttInfo Statistics::getAttInfo(std::string attr){

	AttInfo aInfo;
	for (auto ip: partitionsMap){
		Partition &p = ip.second;
		for (auto ia: p.AttributeMap){
			std::string p_attr = ia.first;
			if (p_attr.compare(attr)==0){
				aInfo.partitionNum = p.partitionNum;
				aInfo.numTuples = p.numTuples;
				aInfo.numDistinct = ia.second;
				return aInfo;
			}
		}		
	}
	aInfo.partitionNum = -1;
	aInfo.numTuples = -1;
	aInfo.numDistinct = -1;
	return aInfo;
}

double Statistics::getCNFSelectivity(std::string attName, std::vector<std::string> &orAttributes, double selFac, int oper){
		double  sel = 1.0f;
		auto attInfo = getAttInfo(attName);
		unsigned long long int Tuples = attInfo.numTuples;
		unsigned long long int disTupAttr = attInfo.numDistinct;
		sel = (oper == EQUALS) ? disTupAttr/Tuples : 1.0/3;

		if (orAttributes.empty()) {
			selFac = sel;
			orAttributes.push_back(attName);
		} else if (std::find(orAttributes.begin(), orAttributes.end(), attName) == orAttributes.end()) {
					selFac = 1 - (1 - selFac) * (1 -sel);
					orAttributes.push_back(attName);
		} else {
					selFac += sel;
		}
		return selFac;
}

double Statistics::JoinCost(const struct AndList *andList) {
	
	long double totalTuples;
	double totSelFactor = 1.0f;
	
	while (andList != NULL) {
		struct OrList *orList = andList->left;
		double orSelFactor = 1.0f;
		std::vector<std::string> orAttributes;  
		while (orList != NULL) {
			struct ComparisonOp *compOP = orList->left;
			int leftOperand = compOP->left->code;
			int rightOperand = compOP->right->code;
			
			int opCode = compOP->code;
			// NAME == NAME
				if (leftOperand ==NAME && rightOperand == NAME) {
					if (opCode != EQUALS) {
						cout<<"Error: Join allowed only with equals operation" <<endl;
						return -1;
					} 
					
					std::string lName = compOP->left->value;
					std::string rName = compOP->right->value;
					auto lAttInfo = getAttInfo(lName);
					auto rAttInfo = getAttInfo(rName);
					if (lAttInfo.partitionNum == -1 || rAttInfo.partitionNum==-1){
						cout<<"Error: not able to find attribute name in any partitions" << endl;
					}
					unsigned long long int lTuples = lAttInfo.numTuples;
					unsigned long long int rTuples = rAttInfo.numTuples;
					//Max of Distinct tuples for given attrtributes
					int max =  std::max(lAttInfo.numDistinct, rAttInfo.numDistinct);
					totalTuples = (double) (((double) (lTuples * rTuples)) / (double) max);

				} else if (leftOperand == NAME ) {
					orSelFactor = getCNFSelectivity(compOP->left->value, orAttributes, orSelFactor, opCode);

				}
				else if (rightOperand == NAME) {
					orSelFactor = getCNFSelectivity(compOP->right->value, orAttributes, orSelFactor, opCode);
				}
			orList = orList->rightOr;
			}
		
		
		totSelFactor *= orSelFactor;
		andList = andList->rightAnd;
		
	}
		return totalTuples*totSelFactor;
}


void Statistics::mergePartitions(Partition p1, Partition p2, int newTuples){

	if (p1.partitionNum == p2.partitionNum) { cout<<"Should never happen" << endl; return;}
	if (p2.partitionNum < p1.partitionNum){
		Partition tmp = p2;
		p2 = p1;
		p1 = tmp;
	}
	//swap if needed and
	//always merge into p1

	p1.numTuples = newTuples;
	auto attrMap = p1.AttributeMap;
	auto consumeAttrMap = p2.AttributeMap;
	for (auto ip: consumeAttrMap){
		attrMap.insert({ip.first, ip.second});
	}
	auto rels = p1.relations;
	auto consumerRels = p2.relations;
	for (auto cRel: consumerRels){
		rels.push_back(cRel);
		relationToPartitionMap.insert({cRel, p1.partitionNum});
	}

	partitionsMap.erase(p2.partitionNum);

}
