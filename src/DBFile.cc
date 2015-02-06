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

void DBFile::Add (Record &rec) {
   /* int ret;
    //Add to Page
    ret = write_page->Append(&rec); 
    if (ret == 0) {
        //Could not fit in page; Add it to File
        heapfile->AddPage(write_page, heapfile->GetLength());
        write_page->EmptyItOut();
        write_page->Append(&rec);
    }
   */
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
    Record temp_rec;

    FILE *tableFile = fopen (loadpath, "r");

    while (temp_rec.SuckNextRecord(&f_schema, tableFile) == 1) {
        Add(temp_rec);
    }
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


int DBFile::GetNext (Record &fetchme) {
    int ret;
    if (read_page == NULL && cur_page == 0) {
        cur_page+=1;
        read_page = new Page();
        heapfile->GetPage(read_page, cur_page);
    }
    ret = read_page->GetCurrent(&fetchme);
    if (ret == 0) {
        cur_page+=1;
        if (cur_page < heapfile->GetLength()) {
            heapfile->GetPage(read_page, cur_page);
            read_page->MoveToStart();
            read_page->GetCurrent(&fetchme);
        }

}
        
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}

