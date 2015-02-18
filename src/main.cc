#include "DBFile.h"
#include <iostream>
#include "Record.h"
#include <stdlib.h>
using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}


extern struct AndList *final;
/*
int main(){
	//load page full of records and write to disk
	Schema mSchema("data/catalog", "lineitem");
	Page page;
	FILE *tableFile = fopen("/cise/homes/sgaddam/db-implementation/tpc-h/lineitem.tbl","r");	Record mRec;
	if(!tableFile) return 0;
	cout << "fopen success" << endl;
	while(true){
		mRec.SuckNextRecord(&mSchema, tableFile);
		mRec.Print(&mSchema);
		if(page.Append(&mRec)==0) break;
		//cout << "No of records in page: " << page.numRecs << endl;	
	}
	off_t offset = 1;
	File mFile;
	mFile.Open(0, "foo.file");
	mFile.AddPage(&page,offset);
	mFile.Close();
	//retrive page from disk and display records
	mFile.Open(1, "foo.file");
	Page rPage;
	Record rRec;
	mFile.GetPage(&rPage, offset);

	while(rPage.GetFirst(&rRec)){
		rRec.Print(&mSchema);
	}
	cout << "END" << endl;
}
*/
int main () {
	
	cout << "TESTING DBFile" << endl;
	fType filetype = heap;
	void *startup;
	cout << "END TESTING" << endl;
	

	// try to parse the CNF
	cout << "Enter in your CNF: ";
  	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
		exit (1);
	}

	// suck up the schema from the file
	Schema lineitem ("data/catalog", "lineitem");

	// grow the CNF expression from the parse tree 
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree (final, &lineitem, literal);
	
	// print out the comparison to the screen
	myComparison.Print ();
	OrderMaker dummy, sortorder;
	myComparison.GetSortOrders (sortorder, dummy);
 	sortorder.Print();
 	dummy.Print();

	// now open up the text file and start procesing it
        /*
	FILE *tableFile = fopen ("tpc-h/lineitem.tbl", "r");

        Record temp;
        Schema mySchema ("catalog", "lineitem");

	//char *bits = literal.GetBits ();
	//cout << " numbytes in rec " << ((int *) bits)[0] << endl;
	//literal.Print (&supplier);

        // read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	int counter = 0;
	ComparisonEngine comp;
 	
        while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
		counter++;
		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}

		if (comp.Compare (&temp, &literal, &myComparison))
                	temp.Print (&mySchema);

        } */

}


