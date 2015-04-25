#include <iostream>
#include "Compiler.h"

using namespace std;
Catalog * Catalog::obj = NULL;
//bool Compiler::DDLQueryFlag=false;
char* Compiler::outFile=NULL;
bool Compiler::runQueryFlag=true;
int main(int argc, char *argv[]) {

		char *fileName = "Statistics.txt";
		Statistics s;
        char *relName[] = { "part", "partsupp", "supplier", "nation", "region"};
        s.AddRel(relName[0],200000);
		s.AddAtt(relName[0], "p_partkey",200000);
		s.AddAtt(relName[0], "p_size",50);

		s.AddRel(relName[1], 800000);
		s.AddAtt(relName[1], "ps_suppkey",10000);
		s.AddAtt(relName[1], "ps_partkey", 200000);
		
		s.AddRel(relName[2],10000);
		s.AddAtt(relName[2], "s_suppkey",10000);
		s.AddAtt(relName[2], "s_nationkey",25);
		
		s.AddRel(relName[3],25);
		s.AddAtt(relName[3], "n_nationkey",25);
		s.AddAtt(relName[3], "n_regionkey",5);

		s.AddRel(relName[4],5);
		s.AddAtt(relName[4], "r_regionkey",5);
		s.AddAtt(relName[4], "r_name",5);
		
		s.Write(fileName);

        

    Catalog* myCatalog=Catalog::getInstance();
       // Catalog* myCatalog=NULL;

    while(true)
    {
        cout<<"\nSql>";
    clock_t start;
  double diff;
  start = clock();


    Parser *_PI = new MyParser(myCatalog);
    Optimizer *_OI = new MyOptimizer(myCatalog) ;
    QueryRunner *_QRI = new MyQueryRunner();
   // _QRI->InorderPrint();
    Compiler *qCompiler = new Compiler(_PI,_OI,_QRI,myCatalog);
    qCompiler->Compile();    
    delete _PI;
    delete _OI;
    delete _QRI;
    delete qCompiler;
    diff = ( clock() - start ) / (double)CLOCKS_PER_SEC;
    cout<<"execution time: "<< diff <<'\n';
    cout<<"\n\n";
    }
    return 0;
    
}
