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
#include "HeapDBFile.h"

bool firstTime = true; //TODO
Pipe* ti_Pipe;
Pipe* to_Pipe;

typedef struct {
	Pipe *pipe;
	Record *rec;
}pipeutil;

//USEFUL example
/*void* call_sockethandler(void* nw) {
    Network* network = static_cast<Network*>(nw);

    void* result = network->SocketHandler(somearg);

    // do something w/ result

    return nullptr;
}

Network nw; // this can't go out of scope though
pthread_create(&thread_id, 0, call_sockethandler, &nw); */


void* producer (void *arg) {

	pipeutil *t = (pipeutil *) arg;

	Record *temp = (Record *) t->rec;
	
	//Pipe *p = new Pipe(100);
	ti_Pipe = t->pipe;
	ti_Pipe->Insert (temp);
	ti_Pipe->ShutDown ();
	cout << " producer: inserted record into the pipe\n";
}

void* consumer (void *arg) {

	pipeutil *t = (pipeutil *) arg;

	Record *temp = (Record *) t->rec;
	
	to_Pipe = t->pipe;
	to_Pipe->Remove (temp);
	to_Pipe->ShutDown ();
	cout << " consumer: removed record from the pipe\n";
}

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
}
/* Called during mode change from write to read
 */
void SortedDBFile::DestroyPipeQ() {
	delete in_pipe;
	delete out_pipe;
	delete sortq;
}
/* Called during mode change from read to write
 */
void SortedDBFile::BuildPipeQ() {
    //in_pipe = new Pipe(100);
    //out_pipe = new Pipe(100);

   // sortq = new BigQ(*in_pipe, *out_pipe, *myOrder, runLength);
	cout << "SortedDBFile.BuildPipeQ() Success!" << endl;
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
    cout << "Sorted DBFile Create called" << f_path<<endl;
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
    myOrder->Print();
	//Write myOrder to metadata file
    myOrder->FilePrint(out);
	//Setup Pipes and BigQ
    //BuildPipeQ();

	//in_pipe = new Pipe(100); //TODO
	//out_pipe = new Pipe(100);
	//in_pipe->Insert(NULL);
	//out_pipe->Insert(NULL);

    heapfile->Open(0, f_path);
    return 1;
}


/* If in write mode, write to in_pipe. If we wrote PAGESIZE*runLength into 
 * in_pipe, Time to get sorted records from out_pipe and write to DBFile
 * If in read mode, just change to write mode, Instantiate BigQ and write to in_pipe
 */
void SortedDBFile::Add (Record &rec) {

	//we can remove this if we constrain mode=Read initially
    	if(firstTime){
		firstTime = false;
		in_pipe = new Pipe(100); //TODO
		out_pipe = new Pipe(100);
		cout << "Add() &in_pipe: " << in_pipe << endl;
		cout << "Add() &out_pipe: " << out_pipe << endl;
		cout << "Add() &myOrder: " << myOrder << endl;
		cout << "Add() runLength: " << runLength << endl;
		sortq = new BigQ(*in_pipe, *out_pipe, *myOrder, runLength);
	}

    cout << "SortedDBFile.Add() Start! mode = " << mode << endl;

    if (mode == Write) {
	in_pipe->Insert(&rec);
    } else if (mode == Read) {
	//sortq would be null; Instantiate it and the pipes
	in_pipe->Insert(&rec);
	mode = Write;
    }     
    /*int ret;
    ret = write_page->Append(&rec); 
    if (ret == 0) {
        //Could not fit in page; Add it to File
	int currlen = heapfile->GetLength();
	int whichpage = currlen==0?0:currlen-1;
        heapfile->AddPage(write_page, whichpage);
        write_page->EmptyItOut();
        write_page->Append(&rec);
    }*/
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

