#include <iostream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

// stub file .. replace it with your own DBFile.cc
DBFile::~DBFile(){
	delete heapfile;
	delete read_page;
	delete write_page;
}
DBFile::DBFile () {
    cur_page = 0;
    read_page = new Page();
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
    heapfile = new File();
    write_page   = new Page();
    heapfile->Open(0, f_path);
}
    
/* Add to records to write page, if write page is full
   write that full page to dbfile, create a new write page and add the record */

void DBFile::Add (Record &rec) {
    int ret;
    ret = write_page->Append(&rec); 
    if (ret == 0) {
        //Could not fit in page; Add it to File
	int currlen = heapfile->GetLength();
	int whichpage = currlen==0?0:currlen-1;
        heapfile->AddPage(write_page, whichpage);
        write_page->EmptyItOut();
        write_page->Append(&rec);
    }
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
    Record temp_rec;

    FILE *tableFile = fopen (loadpath, "r");

    while (temp_rec.SuckNextRecord(&f_schema, tableFile) == 1) {
        Add(temp_rec);
    }
    #if 0
        Page rPage;
        Record rRec;
        off_t offset = 1;
        heapfile->GetPage(&rPage, offset);
        while(rPage.GetFirst(&rRec))
                rRec.Print(&f_schema);
        cout << "END" << endl;
    #endif
}

int DBFile::Open (char *f_path) {
    //todo:check again
    heapfile->Open(1, f_path);
}

void DBFile::MoveFirst () {
    //If no pages in File, moveFirst to write Page
    // If Pages in File,  get the first page from file and movetoStart

    if (heapfile->GetLength() == 0 ) {
        heapfile->GetPage(read_page, 1);
        cur_page = 1;
    }   //Move to Start however
        read_page->MoveToStart();
    
}

int DBFile::Close () {
    heapfile->Close();
}

/*
 * Fetches the (next) record from DBFile in the read_page
 * if we read end in read_page, fetch next page from DBFile
 * and get first record
 */
int DBFile::GetNext (Record &fetchme) {
	int ret;
	cout << "heapfile pages: " << heapfile->GetLength() << endl;
	if ( cur_page == 0) {
		cur_page+=1;
       		 if(heapfile->GetLength()!=0){
			cout << "Getting Page " << cur_page <<endl;
			heapfile->GetPage(read_page, cur_page-1);
		 }else{
			//we have records in read_page but DBFile is empty, so read records from write_page
		 }
	
    	}
	cout << "got page? cur_page: " << cur_page << endl;	
	cout << "Nof of recs in cur_page: " << read_page->GetNumRecs() << endl;
        read_page->MoveToStart(); //since get page advances the twoway list pointer, in FromBinary()
    	ret = read_page->GetCurrent(&fetchme);
	cout << "got record? ret: " << ret << endl;
    	if (ret == 0) {
        	cur_page+=1;
		//Get next page from file to read_page and get the first record
        	if (cur_page < heapfile->GetLength()) {
            		heapfile->GetPage(read_page, cur_page-1);
            		read_page->MoveToStart();
            		read_page->GetCurrent(&fetchme);
	    		ret = 1;
        	} else {
	    		//we have reached end in DBFile, if we have some info in write_page, getRecord from it   
		}
    	}
   	return ret;   
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}

