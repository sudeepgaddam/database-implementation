#include <iostream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedDBFile.h"
#include "Defs.h"
#include <sstream>

ComparisonEngine comparisonEng;

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
    myOrder = new OrderMaker();

	sortedheapfile = new HeapDBFile();

}


int SortedDBFile::GetFromMetaData (ifstream &ifs) {
	string line;
	if (ifs.is_open()) {
	    getline (ifs,line); //"sorted"
	    getline (ifs,line); // runLength
	    std::stringstream s_str( line);
    	    s_str >> runLength;
	    myOrder->PutFromFile(ifs);
	}
}

/* 
 * in Write mode during creation; 
 * Add this info to metadata too
 * Setup Bigq using the ordermaker and num_runs provided
 */
int SortedDBFile::Create (char *f_path, fType f_type, void *startup) {
	//Make the mode as write
	//mode = Write; //TODO

	mode = Read; //while Adding record, we are taking care of initial Write!

	SortInfo *sortinfo;
	cout << "Sorted DBFile Create called: " << f_path<<endl;
	char tbl_path[100];
	sprintf (tbl_path, "%s.meta", f_path);
	ofstream out(tbl_path);
	if(!out ) {
	    cout << "Couldn't open file."  << endl;
	}
	out << "sorted" <<endl;
	sortinfo  = (SortInfoDef *)startup;
	runLength = sortinfo->runLength;
	//Copy myOrder
	*myOrder = *(sortinfo->myOrder);
	out << runLength << endl;
	//myOrder->Print();
	//Write myOrder to metadata file
	myOrder->FilePrint(out);
	//Setup Pipes and BigQ
	//BuildPipeQ();

	heapfile->Open(0, f_path);

	sortedheapfile->Create("sortedheapfile.bin", heap, NULL);
	sortedheapfile->Open("sortedheapfile.bin");
	
	return 1;
}


/* If in write mode, write to in_pipe. If we wrote PAGESIZE*runLength into 
 * in_pipe, Time to get sorted records from out_pipe and write to DBFile
 * If in read mode, just change to write mode, Instantiate BigQ and write to in_pipe
 */
void SortedDBFile::Add (Record &rec) {

	if(mode == Read) {
		SwitchMode();
	}
	in->Insert(&rec);
}

void SortedDBFile::Load (Schema &f_schema, char *loadpath) {
    Record temp_rec;

    FILE *tableFile =  fopen(loadpath, "r");
    
    if(tableFile){
    	while (temp_rec.SuckNextRecord(&f_schema, tableFile) == 1) {
        	Add(temp_rec);
    	}
    }
}

int SortedDBFile::Open (char *f_path) {
    cout <<"Sorted file opening from " << f_path <<endl;
    char tbl_path[100];
    sprintf (tbl_path, "%s.meta", f_path);

    ifstream in(tbl_path);
    if(!in) {
       cout << "Couldn't open file."  << endl;
    }
	//Write myOrder ordermaker and runlength from meta data file
    GetFromMetaData(in);
    heapfile->Open(1, f_path);

}

void SortedDBFile::MoveFirst () {
	if (mode == Write) {
		SwitchMode();
	} else if (mode == Read) {
		return sortedheapfile->MoveFirst();
	}
}

int SortedDBFile::Close () {
    if (mode == Write) {
		SwitchMode();
	} else if (mode == Read) {
		return sortedheapfile->Close();
	}
}

/*
 * Fetches the (next) record from DBFile in the read_page
 * if we read end in read_page, fetch next page from DBFile
 * and get first record
 */
int SortedDBFile::GetNext (Record &fetchme) {
	//read data from outpipe and store it in sortedheapfile!
	cout << "GetNext mode= " << mode << endl;
	if (mode == Write) {
		cout << "GetNext Write!" << endl;
		SwitchMode();
	} else if (mode == Read) {
		cout << "GetNext Read!" << endl;
		return sortedheapfile->GetNext(fetchme);
	}
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


/* Merges records from sortedheapfile with BigQ.outPipe and store in tempFile;
*/
void SortedDBFile::Merge(){

	//HeapDBFile *tempFile = new HeapDBFile();
	//tempFile.Create();
	//tempFile.Open();

	cout << "Merge() Start!" << endl;

	sortedheapfile->MoveFirst();
	bool fileEmpty = sortedheapfile->isEmpty();
	cout <<"is file empty?: " << fileEmpty << endl;
	Record filerec;
	Record piperec;
	int fileStatus;
	int pipeStatus;

	bool one_ends = false;

	if(!fileEmpty){

		while(true){
	
			cout << "Merge() Iterate!" << endl;

			fileStatus = sortedheapfile->GetNext(filerec);
			pipeStatus = out->Remove(&piperec);

			cout << "fileStatus: " << fileStatus << endl;
			cout << "pipeStatus: " << pipeStatus << endl;

			one_ends = (fileStatus == 0) || (pipeStatus == 0);

			if(one_ends)
				break;

			int compStatus = comparisonEng.Compare(&filerec, &piperec, myOrder);
			if(compStatus <0){
				//tempFile.Add(filerec);
				cout << "File Rec Smaller: " << endl;
				filerec.Print();
			}else{
				cout << "Pipe Rec Smaller: " << endl;
				//tempFile.Add(piperec);
				piperec.Print();
			}
		}
		if(fileStatus == 0){
		
		}else if(pipeStatus == 0){
		
		}

	}else{
		cout << "0. Dump outpipe into tempFile" << endl;
		while(out->Remove(&piperec)) {
			//tempFile.Add(piperec);
			cout << "1. Dump outpipe into tempFile" << endl;
			piperec.Print();
		}
	}
	
	cout << "Merge() Success!" << endl;

}

/*

Psuedo-code to implement

Add(){
	if(mode=w){
		BigQ.Insert();
	}
	if(mode=r){
		check that BigQ is Empty! //BigQ should be empty - check is just to ensure that
		BigQ.Create();
		inpipe.Add(rec);
		mode=w;
	}
}

Load(){
	iteratively
		Add();
}

MoveFirst, Close, GetNext:
	if(mode=w){
		merge(BigQ, sorted_data);
		mode = r;
	}
	if(mode=r){
		//respective functionality
	}

merge(BigQ, sorted_data){
	while(either ptr=null){
		*ptr1 = outpipe[i];
		*ptr2 = sorted_data.GetFirst(rec);
		compareRecords(*ptr1, *ptr2);
		newFile.Insert(smallestRecord);
		increment corresponding ptr;
	}
	while(increment non-null ptr)
		newFile.Insert(ptr);
}

*/

void *run_q (void *arg) {
	bigq_util *t = (bigq_util *) arg;
	BigQ b_queue(*(t->inpipe),*(t->outpipe),*(t->sort_order),t->run_len);
}

void SortedDBFile::SwitchMode() {
	if(mode == Read) {
		//cout<<"in reading"<<endl;
		mode = Write;
		in = new  (std::nothrow) Pipe(PIPE_BUFFER);
		out = new (std::nothrow) Pipe(PIPE_BUFFER);
		util = new bigq_util();
		util->inpipe=in;
		util->outpipe=out;
		util->sort_order=myOrder;
		util->run_len=runLength;
		pthread_create (&thread1, NULL,run_q, (void*)util);
	}
	else if(mode == Write)  {
		//cout<<"in writing"<<endl;
		mode = Read;
		in->ShutDown();
		//Merge_with_q();
		Merge();
		delete util;
		delete in;
		delete out;
		in=NULL;
		out=NULL;
	}
}

