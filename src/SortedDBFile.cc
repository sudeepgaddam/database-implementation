#include <iostream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedDBFile.h"
#include "Defs.h"

SortedDBFile::~SortedDBFile(){
	cout << "Sorted DBFile DESTRUCTOR" << endl;
	delete heapfile;
	delete read_page;
	delete write_page;
}
SortedDBFile::SortedDBFile () {
    cout << "Sorted DBFile Constructor called" << endl;
    cur_page = 0;
    read_page = new Page();
    heapfile = new File();
    write_page   = new Page();
}

int SortedDBFile::Create (char *f_path, fType f_type, void *startup) {
    cout << "Sorted DBFile Create called" << f_path<<endl;
    heapfile->Open(0, f_path);
    return 1;
}
    
/* Add to records to write page, if write page is full
   write that full page to dbfile, create a new write page and add the record */

void SortedDBFile::Add (Record &rec) {
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

void SortedDBFile::Load (Schema &f_schema, char *loadpath) {
    Record temp_rec;

    FILE *tableFile =  fopen(loadpath, "r");
    
    if(tableFile){
    	while (temp_rec.SuckNextRecord(&f_schema, tableFile) == 1) {
        	Add(temp_rec);
    	}
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

int SortedDBFile::Open (char *f_path) {
    //todo:check again
	cout <<"Sorted file opening from " << f_path <<endl;
    heapfile->Open(1, f_path);
    return 1;
}

void SortedDBFile::MoveFirst () {
        cur_page = 0;
}

int SortedDBFile::Close () {
    return (heapfile->Close() < 0)?0:1;
}

/*
 * Fetches the (next) record from DBFile in the read_page
 * if we read end in read_page, fetch next page from DBFile
 * and get first record
 */
int SortedDBFile::GetNext (Record &fetchme) {
	int ret = 0;
	#ifdef DBFile_Debug
	cout << "heapfile pages: " << heapfile->GetLength() << endl;
	#endif
	if ( cur_page == 0) {
       		 if(heapfile->GetLength() > 0){
	#ifdef DBFile_Debug
			cout << "Getting Page " << cur_page <<endl;
	#endif
		        cur_page+=1;
            		read_page->EmptyItOut();
			heapfile->GetPage(read_page, cur_page-1);
                        read_page->MoveToStart(); //since get page advances the twoway list pointer, in FromBinary()
		 }else{
			if (write_page == NULL) {
			    cout << "Error:DBFile not initialised" << endl;
			    return 0;
			} else if (write_page->GetNumRecs() > 0) {
		            //Copy write page to read page
			    //cout << "Reading Records from write page" << endl;
                            heapfile->AddPage(write_page, cur_page);
                            write_page->EmptyItOut();
            		    read_page->EmptyItOut();
			    heapfile->GetPage(read_page, cur_page);
                            read_page->MoveToStart(); //since get page advances the twoway list pointer, in FromBinary()
		            cur_page+=1;
 			}
		 }
    	}
	#ifdef DBFile_Debug
		cout << "got page? cur_page: " << cur_page << endl;	
		cout << "No of recs in cur_page: " << read_page->GetNumRecs() << endl;
        #endif

    	ret = read_page->GetCurrent(&fetchme);
    	if (ret == 0) {
		/*
		 * If curpage is 1, load next page from file only if File length is 3 because 
		 * there is an empty page 
		 */
        	if (cur_page < (heapfile->GetLength() -1)) {
            		read_page->EmptyItOut();
			heapfile->GetPage(read_page, cur_page);
            		read_page->MoveToStart();
            		read_page->GetCurrent(&fetchme);
		        cur_page+=1;
	    		ret = 1;
		} else if (write_page->GetNumRecs() > 0) {
                            heapfile->AddPage(write_page, cur_page);
                            write_page->EmptyItOut();
            		    read_page->EmptyItOut();
			    heapfile->GetPage(read_page, cur_page);
                            read_page->MoveToStart(); //since get page advances the twoway list pointer, in FromBinary()
            		    read_page->GetCurrent(&fetchme);
		            cur_page+=1;
			    ret = 1;
			//Copy write page to read page;Mark dirty
	    		//we have reached end in DBFile, if we have some info in write_page, getRecord from it   
		}
    	}
   	return ret;   
}

int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	int ret;
        ComparisonEngine comp;
	do {
	    if (GetNext(fetchme) == 1) 
            	ret = comp.Compare (&fetchme, &literal, &cnf); 
	    else return 0;
	} while(ret == 0);
	return ret;
}

int SortedDBFile:: GetPage (Page *putItHere, off_t whichPage) {
	//cout << "heapfile length" << heapfile->GetLength() << endl;;
	if (whichPage < heapfile->GetLength()-1) {
		return heapfile->GetPage(putItHere, whichPage);
	}
	else if(whichPage <= heapfile->GetLength()){
		heapfile->AddPage(write_page, cur_page);
                write_page->EmptyItOut();
		return heapfile->GetPage(putItHere, whichPage);
	}
	return 0; //whichPage out of range
}
