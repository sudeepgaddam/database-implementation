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
	//cout << "new Partitoin number: " << p.partitionNum << endl;
	p.numTuples = numTuples;
	p.relations.push_back(relName);
	//update data structs
	//partitionsMap.insert(std::make_pair(p.partitionNum, p));
	partitionsMap.insert({p.partitionNum, p});

	relationToPartitionMap.insert({relName, p.partitionNum});
	
}
//map
void Statistics::AddAtt(char *relName, char *attName, long numDistincts)
{	
	auto got = relationToPartitionMap.find(relName);
	string relAtt (relName);
	relAtt += '.' + std::string(attName);
	if (got == relationToPartitionMap.end()){
		cout<<"RelName not present in DB" << endl;
	}else{
		int partitionNum = got->second;
		auto got = partitionsMap.find(partitionNum);
		if (got == partitionsMap.end()){
			cout << "PartitionObject was not created for this relName"<<endl;
		}else{
			Partition &p = got->second;						
			p.AttributeMap.insert({relAtt, numDistincts});			
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
			//copy AttributeMap but with newName.AttributeName
			for (auto attP : p.AttributeMap) {
				 int Pos = attP.first.find('.');
				 std::string attName(newName);
				 attName += '.' + attP.first.substr(Pos+1);
				 newp.AttributeMap.insert({attName, attP.second});
			}
			newp.relations.push_back(newName);

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
	outfile << partitionsMap.size() << endl;
	//cout << "partitionsMap.size() " << partitionsMap.size() << endl;
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
	auto estPair = JoinCost(parseTree, relNames,  numToJoin);
	if (estPair.first.first == -1){
		cout << "cannot Apply - check input" << endl;		
		return;
	}
	
	int part1 =	estPair.second.first;
	int part2 =	estPair.second.second;

	//cout << "part1: " << part1 << " part2: " << part2;
	
	if (part1 != -1 && part2 == -1) {
		//Selecion operation
		auto got = partitionsMap.find(part1);
		if (got != partitionsMap.end()) {
			got->second.numTuples = estPair.first.first*estPair.first.second;
			//cout << "got->second.numTuples: " << got->second.numTuples << endl;
		} else {
			//Error finding partition
		}
		return;
	} else if (part1 == -1 && part2 == -1) {
		cout << "JoinCost() returned error" << endl;
		return;
	}
	Partition &p1 = getPartition(part1);
	Partition &p2 = getPartition(part2);
	
	
	mergePartitions(p1, p2, estPair.first.first, estPair.first.second);
	
	
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{

	//bool valid = checkRelNames(relNames, numToJoin);
	//if (!valid) {cout << "Given RelName not found in DB" << endl; return -1;}

	auto estPair = JoinCost(parseTree, relNames, numToJoin );
	return estPair.first.first*estPair.first.second;
	
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
AttInfo Statistics::getAttInfo(std::string attr, char **relNames, int numToJoin){
	AttInfo aInfo;
	for (auto ip: partitionsMap){
		for (auto ap: ip.second.AttributeMap){
			if (ap.first.compare(attr)==0){
				//check if relation name maches with relnames
				//cout << "ip.second.relaion.size" << ip.second.relations.size() << endl;
				aInfo.partitionNum = ip.first;
				aInfo.numTuples = ip.second.numTuples;
				aInfo.numDistinct = ap.second;
				//cout << "aInfo.partNum: " << aInfo.partitionNum << " aInfo.numTuples: " << aInfo.numTuples <<"aInfo.numDistinct"<< aInfo.numDistinct<< endl;
				return aInfo;	
			}
		}
	}	
	//cout << "Attribute Name: ' " <<attr << "'" << "not found" <<endl;
	aInfo.partitionNum = -1;
	aInfo.numTuples = -1;
	aInfo.numDistinct = -1;
	return aInfo;
}

double Statistics::getCNFSelectivity(std::string attName, std::vector<std::string> &orAttributes, double selFac, int oper, char **relNames, int numToJoin){
		double  sel = 1.0f;
		auto attInfo = getAttInfo(attName, relNames, numToJoin);
		unsigned long long int Tuples = attInfo.numTuples;
		unsigned long long int disTupAttr = attInfo.numDistinct;
		sel = (oper == EQUALS) ? 1.0/disTupAttr : 1.0/3;

		if (orAttributes.empty()) {
			selFac = sel;
			orAttributes.push_back(attName);
			//cout<< "Vector Empty" << endl;
		} else if (std::find(orAttributes.begin(), orAttributes.end(), attName) == orAttributes.end()) {
					selFac = 1 - (1 - selFac) * (1 -sel);
					orAttributes.push_back(attName);
					//cout << "Attribute not already present in this ORLIST " <<endl;
		} else {
					selFac += sel;
		}
		return selFac;
}

std::pair<std::pair<unsigned long long int, double>, std::pair<int,int>> Statistics::JoinCost(const struct AndList *andList, char **relNames, int numToJoin) {
	
	long double totalTuples;
	double totSelFactor = 1.0f;

	int partNum1 = -1;
	int partNum2 = -1;
	int selectPartNum1 = -1;
	int selectPartNum2 = -1;

	
	bool hasJoin = false;
	long double selectionTuples = 0;
	
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
						return make_pair(make_pair(-1,-1), make_pair(-1,-1));
					} 
					
					hasJoin = true;
					std::string lName = compOP->left->value;
					std::string rName = compOP->right->value;
					auto lAttInfo = getAttInfo(lName, relNames, numToJoin);
					auto rAttInfo = getAttInfo(rName, relNames, numToJoin);
					//cout << " lName: " << lName << " rName" << rName << endl; 
					if (lAttInfo.partitionNum == -1 || rAttInfo.partitionNum==-1){
						cout<<"Error: not able to find attribute name in any partitions" << endl;
						partNum1 = -1;
						partNum2 = -1;
						return make_pair(make_pair(totalTuples, totSelFactor), make_pair(partNum1, partNum2));
					}
					unsigned long long int lTuples = lAttInfo.numTuples;
					unsigned long long int rTuples = rAttInfo.numTuples;
					//Max of Distinct tuples for given attrtributes
					int max =  std::max(lAttInfo.numDistinct, rAttInfo.numDistinct);
					totalTuples = (double) (((double) (lTuples * rTuples)) / (double) max);

					partNum1 = lAttInfo.partitionNum;
					partNum2 = rAttInfo.partitionNum;

				} else if (leftOperand == NAME ) {
					orSelFactor = getCNFSelectivity(compOP->left->value, orAttributes, orSelFactor, opCode, relNames, numToJoin);
					//cout << "Or Select factor " << orSelFactor << endl;
					auto lAttInfo = getAttInfo(compOP->left->value, relNames, numToJoin);
					selectionTuples = lAttInfo.numTuples;
					selectPartNum1 = lAttInfo.partitionNum;

				}
				else if (rightOperand == NAME) {
					//cout << "Or Select factor " << orSelFactor << endl;
					orSelFactor = getCNFSelectivity(compOP->right->value, orAttributes, orSelFactor, opCode, relNames, numToJoin);
					auto rAttInfo = getAttInfo(compOP->right->value, relNames, numToJoin);
					selectionTuples = rAttInfo.numTuples;
					selectPartNum2 = rAttInfo.partitionNum;
				}
			orList = orList->rightOr;
			}
		orAttributes.clear();
		//cout << "orSelFactor: " << orSelFactor << "totSelFactor: " << totSelFactor<<endl;
		totSelFactor *= orSelFactor;
		andList = andList->rightAnd;
		
	}
	
	if(!hasJoin){
		
		totalTuples = selectionTuples;
		partNum1 = selectPartNum1;
		partNum2 = selectPartNum2;
	}
	
	return make_pair(make_pair(totalTuples, totSelFactor), make_pair(partNum1, partNum2));
	
		//return totalTuples*totSelFactor;
}


void Statistics::mergePartitions(Partition &p1, Partition &p2, double newTuples, double totSelFactor){

	//cout << "startMerge" << endl;
/*
	for (auto ip: partitionsMap){
		cout << "pNum: " << ip.first << " tuples: " << ip.second.numTuples << endl;
	}
	*/
	//cout << "start" << endl;

	int oldPartNum = p2.partitionNum;
	if (p1.partitionNum == p2.partitionNum) { cout<<"Should never happen" << endl; return;}
	//swap if needed and
	//always merge into p1

	p1.numTuples = newTuples*totSelFactor;
	for (auto cRel: p2.relations){
		//cout<<"cRel: " << cRel << "Updating to " << p1.partitionNum <<endl;
		relationToPartitionMap.erase(cRel);
		relationToPartitionMap.insert({cRel, p1.partitionNum});
	}
	//Update existting attribute distinct values
	for (auto got : p1.AttributeMap) {
		//cout << "In Merge Partitions: New attribute value" << got.second*totSelFactor << "Selectivity factor: " <<totSelFactor<< endl;
		p1.AttributeMap.at(got.first) =  got.second*totSelFactor;
	}
	
	for (auto got : p2.AttributeMap) {
		//cout << "In Merge Partitions: New attribute value" << got.second*totSelFactor << "Selectivity factor: " <<totSelFactor<< endl;
		p1.AttributeMap.insert({got.first, got.second*totSelFactor});
	}
	
	
	for (auto got : p2.relations) {
		p1.relations.push_back(got);
	}
	

	partitionsMap.erase(p2.partitionNum);

	/*
	 for (auto ip: partitionsMap){
		cout << "pNum: " << ip.first << " tuples: " << ip.second.numTuples << endl;
	}
	cout << "endMerge" << endl;
	*/
}

Partition& Statistics::getPartition(int partNum){
	auto got = partitionsMap.find(partNum);
	if (got == partitionsMap.end()){
		cout << "Error: cant find partition1" << endl;
		//return;
	}
	return got->second;
	
}
