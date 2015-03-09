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

/*
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

/ 
 // in Write mode during creation; 
 // Add this info to metadata too
 // Setup Bigq using the ordermaker and num_runs provided
//
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


/// If in write mode, write to in_pipe. If we wrote PAGESIZE*runLength into 
 // in_pipe, Time to get sorted records from out_pipe and write to DBFile
 // If in read mode, just change to write mode, Instantiate BigQ and write to in_pipe
 //
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

//
 // Fetches the (next) record from DBFile in the read_page
 // if we read end in read_page, fetch next page from DBFile
 // and get first record
 
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


// Merges records from sortedheapfile with BigQ.outPipe and store in tempFile;

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
*/

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
*/

void *run_q (void *arg) {
	bigq_util *t = (bigq_util *) arg;
	BigQ b_queue(*(t->in),*(t->out),*(t->sort_order),t->run_len);
}

SortedDBFile::SortedDBFile() {
	search_done=0;
};

SortedDBFile::~SortedDBFile() {
};

int SortedDBFile::Open (char *f_path) {
	//cout<<"Open "<<f_path<<endl;
	fpath=f_path;
	try {
		data_file.Open(1,f_path);
//		cout<<"wtf1"<<endl;

		string s=f_path;
		s=s.substr(0,s.length()-4)+".meta";
		char* m_filename=(char*)s.c_str();
		ifstream metafile;
		metafile.open(m_filename,ios::in);
		string file_type; // variable to hold file_type
		string temp;

		getline(metafile,file_type);

		getline(metafile,temp);

		runlen=atoi(temp.c_str());

		getline(metafile,temp);
		sort_info=new OrderMaker();
		sort_info->numAtts=atoi(temp.c_str());

		int count=0;

//		cout<<"wtf2"<<endl;

		while(metafile.good()) {
			getline(metafile,temp);
			sort_info->whichAtts[count]=atoi(temp.c_str());
			getline(metafile,temp);
			sort_info->whichTypes[count]=(Type)atoi(temp.c_str());
			count++;
		}
		mode = reading;
/*
		in = new Pipe(PIPE_BUFFER);
		out = new Pipe(PIPE_BUFFER);
//		cout<<"wtf3"<<endl;

		bigq_util util = {in, out, sort_info, runlen};

		pthread_t thread1;

		pthread_create (&thread1, NULL,run_q, (void*)&util);
//		cout<<"wtf5"<<endl;

		//cout<<"5\n";
*/
		return 1;
	} catch (exception &e) {
		cout<<"Error in Sorted File Open"<<endl;
	}
}

void SortedDBFile::Add_New_File(Record &rec) {
	if(!temp_buffer_page.Append(&rec)) {
		temp_data_file.AddPage(&temp_buffer_page,temp_buffer_ptr++);
		temp_buffer_page.EmptyItOut();
		temp_buffer_page.Append(&rec);
	}
}
int SortedDBFile::GetNext_File(Record &fetchme) {
	while(!buffer_page.GetFirst(&fetchme)) {
		if(cur_page<data_file.GetLength()-2) {
			data_file.GetPage(&buffer_page,++cur_page);
		}
		else
			return 0;
	}
	return 1;
}

void SortedDBFile::Merge_with_q() {
	Record rec1;
	Record rec2;
	ComparisonEngine comp;
	temp_data_file.Open(0,"temp_dbfile.bin");
	cur_page=0;
	temp_buffer_ptr=0;
	int file_empty=0;
	int frec_status=0;
	int qrec_status=0;

	if(data_file.GetLength()>0) {
		data_file.GetPage(&buffer_page,0);
	}
	else file_empty=1;

	bool done=false;

	if(!file_empty) {
		frec_status=GetNext_File(rec1);

		qrec_status=out->Remove(&rec2);

		while(!done) {

			if(frec_status && qrec_status) {
//				if (!rec1.bits ) {
//					cout<<"caught u\n";
//				}
				if(comp.Compare(&rec1,&rec2,sort_info)<0) {
					//if(!rec1.bits) cout<<"can't add rec1 in 1st if\n";

					Add_New_File(rec1);
					frec_status=GetNext_File(rec1);
				}
				else {
				//	if(!rec2.bits) cout<<"can't add rec2 in 1st else\n";

					Add_New_File(rec2);
					qrec_status=out->Remove(&rec2);
				}
			}
			else if(frec_status) {
				do {
					//if(!rec1.bits) cout<<"can't add rec1 in 1st elsif\n";
					Add_New_File(rec1);
				} while(GetNext_File(rec1));
				done=true;
			}
			else if(qrec_status) {
				do {
					//if(!rec2.bits) cout<<"can't add rec2 in 2nd elsif\n";

					Add_New_File(rec2);
				} while(out->Remove(&rec2));
				done=true;
			}
			else {
				done=true;
			}
		}
	}
	else {
		while(out->Remove(&rec2)) {
			//if(!rec2.bits) cout<<"can't add rec2 in outer else\n";

			Add_New_File(rec2);
		}
	}

	temp_data_file.AddPage(&temp_buffer_page,temp_buffer_ptr);
	rename("temp_dbfile.bin",fpath);
	temp_data_file.Close();
}

int SortedDBFile::Close () {
	if(mode == writing) {
		SwitchMode();
	}
	return data_file.Close();
}

int SortedDBFile::Create(char *f_path, fType f_type, void *startup) {
	fpath=f_path;
	try {
		//cout<<"in sorted create\n";
		data_file.Open(0,f_path);
		//cout<<"1\n";
		//Initialize OrderMaker
		sort_info=((Sort_Info*)startup)->myOrder;
		runlen=((Sort_Info*)startup)->runLength;

		string s=f_path;
		s=s.substr(0,s.length()-4)+".meta";
		char* m_filename=(char*)s.c_str();
		ofstream metafile;
		metafile.open(m_filename,ios::trunc);
		metafile<<"sorted"<<"\n";
		metafile<<runlen<<"\n";
		metafile<<sort_info->numAtts;
		for(int i=0;i<sort_info->numAtts;i++) {
			metafile<<"\n"<<sort_info->whichAtts[i]<<"\n"<<sort_info->whichTypes[i];
		}
		mode = reading;
		/*
		cout<<"2\n";
		in = new Pipe(PIPE_BUFFER);
		out = new Pipe(PIPE_BUFFER);
		cout<<"3\n";
		bigq_util util = {in, out, sort_info, runlen};
		pthread_t thread1;
		cout<<"4\n";
		//pthread_create (&thread1, NULL,run_q, (void*)&util);
		cout<<"5\n";
		//pthread_join (thread1, NULL); */
		return 1;
	} catch (exception &e) {
		cout<<"Error in Sorted File create"<<endl;
	}
}

void SortedDBFile::Load (Schema &f_schema, char *loadpath) {
	if(mode == reading) {
		SwitchMode();
	}
	FILE *fp=fopen(loadpath,"r");
	Record r;
	int cnt=0;
	while(r.SuckNextRecord(&f_schema,fp)) {
		cnt++;
		Add(r);
		//cout<<cnt<<endl;
	}
}
void SortedDBFile::MoveFirst () {
	if(mode == writing) {
		SwitchMode();
	}
	cur_page=0;
	search_done=0;
	if(data_file.GetLength()>0)
		data_file.GetPage(&buffer_page,0);
	else
		buffer_page.EmptyItOut();
}

void SortedDBFile::Add (Record &rec) {
	if(mode == reading) {
		SwitchMode();
	}
	//cout<<"bef insert"<<endl;
	in->Insert(&rec);
}

int SortedDBFile::GetNext(Record &fetchme) {
	if(mode == writing) {
		SwitchMode();
		MoveFirst();
	}
	while(!buffer_page.GetFirst(&fetchme)) {
		if(cur_page<data_file.GetLength()-2) {
			data_file.GetPage(&buffer_page,++cur_page);
		}
		else
			return 0;
	}
	return 1;
}


int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	if(mode == writing) {
		SwitchMode();
		MoveFirst();
	}
	OrderMaker dummy;
	OrderMaker query_order;
	cnf.GetSortOrders(query_order,dummy);
	OrderMaker search_order;
	OrderMaker literal_order;
	ComparisonEngine comp;
	bool accept_attr = false;


	for (int i = 0; i < sort_info->numAtts; i++) {
		int att = sort_info->whichAtts[i];
		int literalIndex = cnf.HasSimpleEqualityCheck(att);
		if (literalIndex != -1) {
			search_order.whichAtts[i]=att;
			search_order.whichTypes[i]=sort_info->whichTypes[i];
			literal_order.whichAtts[i]=literalIndex;
			literal_order.whichTypes[i]=sort_info->whichTypes[i];
			search_order.numAtts++;
			literal_order.numAtts++;
		}
		else {
			break;
		}
	}

	//search_order.Print();
	//literal_order.Print();
	if(search_order.numAtts==0) {
		cout<<"no bs\n";
		return GetNext_NO_BS(fetchme, cnf, literal);
	}
	if(search_done) {
		cout<<"in search done\n";
		return GetNext_With_BS(fetchme,cnf,literal,search_order,literal_order);
	}
	cout<<"gng for 1st match\n";
	int f_page;
	int l_page;
	int m_page;
	int file_length;

	if(data_file.GetLength()) {
		file_length=data_file.GetLength()-1;
	}
	else {
		return 0;
	}

	f_page=0;
	l_page=file_length-1;

	if(file_length==1) {
		return GetNext_NO_BS(fetchme, cnf, literal);
	}
//	cout<<"hmm1"<<endl;
//	literal.Print(&Schema("catalog","lineitem"));
	//cout<<"hmm"<<endl;
	while(f_page<(l_page-1)) {
		m_page=(f_page+l_page)/2;
		//cout<<"mpage: "<<m_page<<endl;
		data_file.GetPage(&buffer_page,m_page);
		buffer_page.GetFirst(&fetchme);
		//fetchme.Print(&Schema("catalog","lineitem"));
		int comparison=comp.Compare (&fetchme,&search_order,&literal, &literal_order);
		//cout<<"comp: "<<comparison<<endl;
		if (comparison>=0) {
			l_page=m_page;
		}
		else if(comparison<0) {
			f_page=m_page;
		}
		else {
			cout<<"Wrong comparison"<<endl;
		}
	}

	search_done=1;

	bool match_found=false;
	//cout<<"fpage: "<<f_page<<" lpage: "<<l_page<<endl;

	data_file.GetPage(&buffer_page,f_page);

	if(GetFirst_Match(fetchme, literal, cnf, search_order,literal_order)) {
		cur_page=f_page;
		match_found=true;
	}
	else {
		//cur_page=f_page;
		data_file.GetPage(&buffer_page,f_page+1);
		if(GetFirst_Match(fetchme, literal, cnf, search_order,literal_order)) {
			cur_page=f_page+1;
			match_found=true;
		}
	}
	if(!match_found) return 0;

	return comp.Compare(&fetchme,&literal,&cnf) || GetNext_With_BS(fetchme,cnf,literal,search_order,literal_order);
}

int SortedDBFile::GetFirst_Match(Record &fetchme, Record &literal, CNF &cnf, OrderMaker &search_order,OrderMaker &literal_order) {
	ComparisonEngine comp;
	//cout<<"getnext first match"<<endl;

	while(buffer_page.GetFirst(&fetchme)) {
		//fetchme.Print(&Schema("catalog","lineitem"));
		if (!comp.Compare (&fetchme,&search_order,&literal, &literal_order)) {
				return 1;
		}
		else {
			continue;
		}
	}
	//return 0;
	//cout<<"ghyuji"<<endl;
	return 0;
}

int SortedDBFile::GetNext_NO_BS(Record &fetchme, CNF &cnf, Record &literal) {
	//cout<<"getnext no bs"<<endl;
	ComparisonEngine comp;
		bool found = false;
		while (!found) {
			while (!buffer_page.GetFirst(&fetchme)) {
				if (cur_page < data_file.GetLength() - 2) {
					data_file.GetPage(&buffer_page, ++cur_page);
				} else
					return 0;
			}
			if (comp.Compare(&fetchme, &literal, &cnf))
				found = true;
		}
		return 1;
}

int SortedDBFile::GetNext_With_BS(Record &fetchme, CNF &cnf, Record &literal,
		OrderMaker &search_order,OrderMaker &literal_order) {
	//cout<<"in getnext with bs\n";
	bool found = false;
	ComparisonEngine comp;

		while (!found) {
			while (!buffer_page.GetFirst(&fetchme)) {
				if (cur_page < data_file.GetLength() - 2) {
					data_file.GetPage(&buffer_page, ++cur_page);
				} else
					return 0;
			}
			if (!comp.Compare (&fetchme,&search_order,&literal, &literal_order)) {
				if(comp.Compare(&fetchme,&literal,&cnf))
					found = true;
				else continue;
			}
			else
				return 0;
		}
		return 1;
}


void SortedDBFile::SwitchMode() {
	if(mode == reading) {
		//cout<<"in reading"<<endl;
		mode = writing;
		in = new  (std::nothrow) Pipe(PIPE_BUFFER);
		out = new (std::nothrow) Pipe(PIPE_BUFFER);
		util = new bigq_util();
		util->in=in;
		util->out=out;
		util->sort_order=sort_info;
		util->run_len=runlen;
		pthread_create (&thread1, NULL,run_q, (void*)util);
	}
	else if(mode == writing)  {
		//cout<<"in writing"<<endl;
		mode = reading;
		in->ShutDown();
		Merge_with_q();
		delete util;
		delete in;
		delete out;
		in=NULL;
		out=NULL;
	}
}
