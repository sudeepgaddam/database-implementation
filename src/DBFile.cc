#include <iostream>
#include <fstream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

DBFile::~DBFile(){
	cout << "DBFile DESTRUCTOR" << endl;
	delete gendbfile;
}
DBFile::DBFile () {
}
/* Based on fType, Instantiate the respective DBFile
 * Leave the metadata filling part to respective create function
 */
int DBFile::Create (char *f_path, fType f_type, void *startup) {
	if (f_type == heap) {
		cout << "Heap file creation" <<endl;
	        gendbfile = new HeapDBFile();
	} else if(f_type = sorted) {
		cout << "Sorted file creation" <<endl;
	        gendbfile = new SortedDBFile();
	}
	gendbfile->Create(f_path, f_type, startup);
}
    
void DBFile::Add (Record &rec) {
	gendbfile->Add(rec);
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	gendbfile->Load(f_schema, loadpath);
}

int DBFile::Open (char *f_path) {
	char tbl_path[100];
        string line;
        sprintf (tbl_path, "%s.meta", f_path);
	ifstream myfile (tbl_path);
	if(!myfile ) {
      	    cout << "Couldn't open metadata file for reading"  << endl;
      	    return 1;
    	}
  	if (myfile.is_open()) {
		if (getline (myfile,line)) {
		    if (line.compare("heap") == 0) {
			cout <<"Creating heap dbfile" <<endl;
	        	gendbfile = new HeapDBFile();
		    } else if (line.compare("sorted") == 0) {
			cout <<"Creating sorted dbfile" <<endl;
	       		gendbfile = new SortedDBFile();
		    }
		}
	}
	gendbfile->Open(f_path);
}

void DBFile::MoveFirst () {
	gendbfile->MoveFirst();
}

int DBFile::Close () {
	gendbfile->Close();
}
int DBFile::GetNext (Record &fetchme) {
	cout << "DBFile.GetNext() Start!" << endl;
	gendbfile->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	gendbfile->GetNext(fetchme, cnf, literal);
}

int DBFile:: GetPage (Page *putItHere, off_t whichPage) {
	gendbfile->GetPage(putItHere, whichPage);
}


