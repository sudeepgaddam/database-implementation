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
#include "DBFile.h"
#include <unistd.h>


ComparisonEngine comparisonEng;
int SortedDBFile::addcount = 0;

SortedDBFile::~SortedDBFile(){
	//cout << "Sorted DBFile DESTRUCTOR" << endl;
	delete heapfile;
	delete read_page;
	delete write_page;
	delete outpipefile;

}

SortedDBFile::SortedDBFile () {
    	//cout << "Sorted DBFile Constructor called" << endl;    
	cur_page = 0;
	outpipefile = new File();
    	read_page = new Page();
    	heapfile = new File();
    	write_page   = new Page();
	myOrder = new OrderMaker();
	heapDB = new HeapDBFile();
	
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

	fpath=f_path;
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
	heapDB->Create(f_path, heap, NULL);
	
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

	fpath=f_path;
	mode = Read;

	//Write myOrder ordermaker and runlength from meta data file
    	GetFromMetaData(in);

	cout << "Opening heap file from path: " <<f_path <<endl;
	cout << "fileMode: " << mode << endl;

    	heapfile->Open(1, f_path);
    	heapDB->Open(f_path);
    	
}

void SortedDBFile::MoveFirst () {
	if (mode == Write) {
		SwitchMode();
	} else if (mode == Read) {
		heapDB->MoveFirst();
		cur_page = 0;		
		//return sortedheapfile->MoveFirst();
	}
}



int SortedDBFile::Close () {

    	if (mode == Write) {
		cout << "SortedDBFile.Close() SwitchMode!" << endl;
		SwitchMode();
	} else if (mode == Read) {
		return (heapfile->Close() < 0)?0:1;		
		//return sortedheapfile->Close();
	}
}

/*
 * Fetches the (next) record from DBFile in the read_page
 * if we read end in read_page, fetch next page from DBFile
 * and get first record
 */
int SortedDBFile::GetNext (Record &fetchme) {
	//read data from outpipe and store it in sortedheapfile!
	
	if (mode == Write) {
		//cout << "SortedDBFile.GetNext Write!" << endl;
		SwitchMode();
	} else if (mode == Read) {
		//cout << "SortedDBFile.GetNext Read!" << endl;
		//Start copying HEAPDBFILE functionality
		return heapDB->GetNext(fetchme);
		//End copying HEAPDBFILE functionality   
		//return sortedheapfile->GetNext(fetchme);
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

void* run_q (void *arg) {
	
	bigq_util *t = (bigq_util *) arg;
	BigQ b_queue(*(t->in),*(t->out),*(t->sort_order),t->run_len);
}


void SortedDBFile::Merge(){

	bool fileEmpty = heapDB->isEmpty();
	cout<<"is File Empty?"<<fileEmpty<<endl;
	off_t whichPage = 0;
	Record filerec;
	Record piperec;

	if(!fileEmpty){
	
		Pipe *in1;
    	Pipe *out1;
		bigq_util* util1;
		pthread_t second_bigq_thread;
		in1 = new  (std::nothrow) Pipe(PIPE_BUFFER);
		out1 = new (std::nothrow) Pipe(PIPE_BUFFER);
		util1 = new bigq_util();
		util1->in=in1;
		util1->out=out1;
		util1->sort_order=myOrder;
		util1->run_len=runLength;
		

		
		
		int outpiperecs = 0;
		HeapDBFile *tempFile = new HeapDBFile();
		char *a = "tmp";
		tempFile->Create(a, heap, NULL);
		while(out->Remove(&piperec)){
			heapDB->Add(piperec);
			outpiperecs++;
		}
		heapDB->FlushWritePage();
		cout << "FirstBigQ outpiperecs: " << outpiperecs << endl;
		

		pthread_join(thread1, NULL);
	
		cout << "### start secondBigQThread()" << endl;
		pthread_create (&second_bigq_thread, NULL,run_q, (void*)util1);

		int filereccount = 0;
		heapDB->MoveFirst();
		while(heapDB->GetNext(filerec)){
			in1->Insert(&filerec);
			filereccount++;
		}
		cout << "filereccount: " << filereccount << endl;
		in1->ShutDown();
		cout << "Second Bigq inPipe Closed" <<endl;
		Record sortedrec;
		filereccount = 0;
		
		while(out1->Remove(&sortedrec)){
			tempFile->Add(sortedrec);
			filereccount++;
		}		
		tempFile->FlushWritePage();
		cout << "Sorted out record count: " << filereccount << endl;
		
		
		delete heapDB;
		//Rename tmp fill
		rename(a,fpath);
		heapDB = tempFile;

	}else{
		cout <<"DumpSortedOutPipeContents into originalheapfile Start!" << endl;
		int reccount = 0;
		cout<<"Merge() out address" << out <<endl;
		while(out->Remove(&piperec)) {
			  heapDB->Add(piperec);
			  reccount++;
		}
		heapDB->FlushWritePage();
		cout << "merge()  removed record count from out pipe " <<reccount<<endl;
	//	DumpWriteBuffer();

	}

}

void SortedDBFile::SwitchMode() {
	if(mode == Read) {
		//cout<<"in reading"<<endl;
		mode = Write;
		in = new  (std::nothrow) Pipe(PIPE_BUFFER);
		out = new (std::nothrow) Pipe(PIPE_BUFFER);
		util = new bigq_util();
		util->in=in;
		util->out=out;
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

void SortedDBFile::FlushWritePage(){
}

