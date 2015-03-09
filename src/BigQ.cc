#include "BigQ.h"
#include <stdlib.h>
#include <vector>
#include <iostream>
#include "DBFile.h"
#include <queue>


OrderMaker *localOrder;
ComparisonEngine compEng;

int Compare (const void *a, const void *b){
	ComparisonEngine comp;
	return comp.Compare((Record *) a, (Record *)b, localOrder);
}


struct RecordInfo{
	Record* rec;
	int bufferId;

	RecordInfo(){
	}
	
	RecordInfo(const RecordInfo& obj){
		//cout << "start RecordInfo c'tor" << endl;
		this->bufferId = obj.bufferId;
		this->rec = new Record();
		*this->rec = *(obj.rec); 
		//cout << "end RecordInfo c'tor" << endl;
	}

	bool operator < (const RecordInfo& rInfo) const{

		//cout << "start compare operator" << endl;
		const Record *reca = rec;
		const Record *recb = (rInfo.rec);
		//cout << "calling ComparisonEngine.Compare()" << endl;
		return Compare((void *) reca,(void *) recb) > 0;
	}
};


void phasetwo(int num_runs, int runlen, DBFile* infile, Pipe *outpipe){
	

	int num_buffers = num_runs; // num of runs	
	vector <Record> outbuffer;
	int outpointer;
	vector<Page> buffers;

	priority_queue<RecordInfo> pq;  //greater than comparison -> min pq
	
	char * fpath = "lineitem.in";
	infile->Open(fpath);
	infile->MoveFirst();

	off_t whichpages[num_buffers];

	cout << "***** start inserting records into priority queue " << endl;
	for(int i=0; i<num_buffers; i++){
		
		//initialize
		whichpages[i] = i*runlen;

		// read one page into each buffer
		Page* m_page = new Page();
		if(!infile->GetPage(m_page, whichpages[i])){
			cout << "ERROR: Not able to read page" << endl;
			continue;
		}
		if(m_page->GetNumRecs() <=0) continue;
		
		buffers.push_back(*m_page); 
			
		// insert current recInfos into pq --TODO boundary conditions			  
		RecordInfo recInfo;
		recInfo.rec = new Record();
		buffers[i].MoveToStart(); //since getpage advances current to end
		//cout <<" 1. initial right length: " << buffers[i].RightLength() << endl;
		if(!buffers[i].GetFirst(recInfo.rec)) { //replaced GetCurrent
			cout << "unable to read current record" << endl;			
			continue;
		}else {
			//recInfo.rec->Print(schema);		 
			recInfo.bufferId = i;		
			pq.push(recInfo);
			//cout <<" 2. initial right length: " << buffers[i].RightLength() << endl;
		}			
		
		delete m_page;
		                
	}

	char * foutpath = "lineitems.sorted";
	DBFile* outfile = new DBFile();
	outfile->Create(foutpath, heap, NULL);
	outfile->Open(foutpath);

	cout << "***** TPMMS logic starts " << endl;
	
	int outbuffersize = 0;
	int one_buffer_left = 0;
	outpointer = -1;

	while(!pq.empty() && !one_buffer_left){
		RecordInfo currentRecInfo = pq.top();
		pq.pop();
		Record* current  = currentRecInfo.rec;
		int bufferindex = currentRecInfo.bufferId;
		//cout << "least buffer index: " << bufferindex << endl;
		//current->Print();
		outbuffer.push_back(*current);
		outbuffersize += current->GetSize();
		outpointer++;
		//if output buffer is full write into file
		if(outbuffersize==PAGE_SIZE){ 
			for(int i=0; i<outbuffer.size();i++){
				//outfile->Add(outbuffer[i]);
				outpipe->Insert(&outbuffer[i]);
			}
			outbuffersize = 0;
			outbuffer.clear();
			outpointer = -1;
		}		

		RecordInfo tempInfo;		
		tempInfo.bufferId = bufferindex;
		tempInfo.rec = new Record();
		//cout <<" 3. initial right length: " << buffers[bufferindex].RightLength() << endl;
		if(!buffers[bufferindex].GetFirst(tempInfo.rec)) { //replaced GetCurrent
			whichpages[bufferindex]++;
			//cout << "reached end of page, so read next page into buffer" << endl;			
			if(whichpages[bufferindex] < (bufferindex+1)*runlen && !infile->GetPage(&buffers[bufferindex], whichpages[bufferindex])){
				cout << "ERROR: Not able to read page" << endl;
				//continue;
			}else if(whichpages[bufferindex] < (bufferindex+1)*runlen){
				buffers[bufferindex].MoveToStart();
				if(buffers[bufferindex].GetFirst(tempInfo.rec)){ //replaced GetCurrent
					pq.push(tempInfo);
				}
			}
		}else{
			//increment pointer to next record
			//cout << "push next record" << endl;
			//cout <<" later right length: " << buffers[bufferindex].RightLength() << endl;
			//tempInfo.rec->Print(schema);
			//cout <<" 4. initial right length: " << buffers[bufferindex].RightLength() << endl;
			pq.push(tempInfo);
		}
		
		if(pq.size()==1){
			one_buffer_left = 1;
			//flush output buffer records even if it is not full - since we are done with all runs
			for(int i=0; i<outbuffer.size();i++){
				//outfile->Add(outbuffer[i]);
				outpipe->Insert(&outbuffer[i]);
			}
			outbuffer.clear();
		}
	}

	cout << "***** Processing last sorted run " << endl;

	if(one_buffer_left){
		//flush priority que record into out file		
		RecordInfo tempInfo = pq.top();
		pq.pop();
		int bufferindex = tempInfo.bufferId;
		//tempInfo.rec->Print(schema);
		//outfile->Add(*tempInfo.rec);
		outpipe->Insert(tempInfo.rec);
		//flush all buffer records into outfile		
		while(true){
			tempInfo.rec = new Record();
			if(buffers[bufferindex].GetFirst(tempInfo.rec)){	//replaced GetCurrent	//need to break into small functions !!
				//cout << "Add buffer record into file" << endl;
				//tempInfo.rec->Print(schema);
				//outfile->Add(*tempInfo.rec);
				outpipe->Insert(tempInfo.rec);
			}else{
				//cout << "End of last buffer!!" << endl;
				break;
			}
		}
		//delete tempInfo;
		Page p;
		whichpages[bufferindex]++;
		while(whichpages[bufferindex] < (bufferindex+1)*runlen  && infile->GetPage(&buffers[bufferindex], whichpages[bufferindex]))	{	
			buffers[bufferindex].MoveToStart();
			Record *rec = new Record();
			while(buffers[bufferindex].GetFirst(rec)){   //replaced GetCurrent
				//rec->Print(schema);
				//outfile->Add(*rec);
				outpipe->Insert(rec);
			}
			whichpages[bufferindex]++;
		}

	}
	cout << "***** Success!! Phase Two Ends " << endl;

	cout << "Print infile records -- should not be in sorted order" << endl;
	infile->MoveFirst();
	Record inrec;
	int inreccount = 0;
	while (infile->GetNext (inrec) == 1) {
		inrec.Print();
		inreccount++;
	}
	cout << "End of infile!! contains-> " << inreccount << " records" << endl;
	/*cout << "Print outfile records -- should be in sorted order" << endl;
	outfile->MoveFirst();
	Record outrec;
	int outreccount = 0;
	while (outfile->GetNext (outrec) == 1) {
		//outrec.Print();
		outreccount++;
	}
	cout << "End of outfile!! contains-> " << outreccount << " records" << endl;*/

	outfile->Close();

}


void *bigqthread (void *args) {

	BigQ *bq=(BigQ *)args;

	//PipeOrders *pipes = (PipeOrders *) arg;
	
	cout << "0 *bigqthread() &in:  " << bq->inPipe << endl;
	cout << "0 *bigqthread() &out: " << bq->outPipe << endl;
	cout << "0 *bigqthread() &sortorder: " << bq->order << endl;
	cout << "0 *bigqthread() runlen: " << bq->runLength << endl;

	Pipe *in = bq->inPipe;
	Pipe *out = bq->outPipe;
	OrderMaker *sortorder = bq->order;
	int runlen = bq->runLength;

	/*Pipe &in = *(pipes->inPipe);
	Pipe &out = *(pipes->outPipe);
	OrderMaker *sortorder = pipes->order;
	int runlen = pipes->runLength;*/
	localOrder = sortorder;
	int count = 0;

	cout << "1 *bigqthread() &in:  " << in << endl;
	cout << "1 *bigqthread() &out: " << out << endl;
	cout << "1 *bigqthread() &sortorder: " << sortorder << endl;
	cout << "1 *bigqthread() runlen: " << runlen << endl;

	// read data from in pipe sort them into runlen pages
	Record rec;
	Record temp;
	Schema *rschema = new Schema ("data/catalog", "region");
	Schema *lschema = new Schema ("data/catalog", "lineitem");
	Schema *psschema = new Schema ("data/catalog", "partsupp");
	Schema *pschema = new Schema ("data/catalog", "partsupp");
    	Schema *schema = pschema;
	DBFile tempFile;
	char * fpath = "lineitem.in";
	tempFile.Create(fpath, heap, NULL);

      	vector<Record> vrec;	
	int runcount = 0;
	int size    = 0;
	int recordcount = 0;	
	tempFile.Open(fpath);

	cout << endl;
	cout << endl;
	cout << "=============== start TPMMS ===============" << endl;

	cout << "***** Start reading from in-pipe" << endl;
	int i = 0;
	
	while(in->Remove(&rec)){
		
		recordcount++;
		int recsize = rec.GetSize();
		size += recsize;
		if(size <= PAGE_SIZE*runlen) {
			vrec.push_back(rec); 
		}
		else{
			runcount++;
			cout << "***** One Run Completed. Run count: " << runcount<<endl;
			cout << "***** " << recordcount << " records present in run: " << runcount << endl;			
	
			qsort((void *) &vrec[0], recordcount-1, sizeof(Record), Compare);
			
			for(int i=0; i<recordcount-1; i++){
				tempFile.Add(vrec[i]);
			}
			vrec.clear();
			size = recsize;
			vrec.push_back(rec);
			recordcount = 1;
		}
	}
	
	cout << "finished reading: recs - " << recordcount << endl;
	cout << "finished reading: runs - " << runcount << endl;
				
	int vsize = recordcount;//vrec.size();
	if(vsize >0) { 
		runcount++;
		//cout << "start qsort" << endl; 
		qsort((void *) &vrec[0], recordcount, sizeof(Record), Compare);
	    	//cout << "end qsort" << endl;
	}
	for(int i=0; i<vsize; i++){
        	tempFile.Add(vrec[i]);
	}
	while (tempFile.GetNext (temp) == 1) {
		
		if (count < 1000) {
			//temp.Print(schema);
		}
		count++;
	}

	vrec.clear();
	tempFile.Close();

	cout << "***** Success!! Phase one ends with runs=qsortcount: " << runcount << endl;
	cout << "***** Infile Records: " << count << endl;

	phasetwo(runcount, runlen, &tempFile, out);
	

	// construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	out->ShutDown ();

	cout << "=============== end  TPMMS ===============" << endl;
	cout << endl;
	cout << endl;
	
}
/*BigQ :: BigQ (Pipe *in, Pipe *out, OrderMaker *sortorder, int runlen) {
	/*PipeOrders pipes;
	pipes.inPipe  = &in;
	pipes.outPipe = &out;
	pipes.order = &sortorder;
	pipes.runLength = runlen;*

	//cout << "0. BigQ c'tor: &in_pipe: " << in << endl;
	//cout << "0. BigQ c'tor: &out_pipe: " << out << endl;
	//cout << "0. BigQ c'tor: &myOrder: " << sortorder << endl;
	cout << "0. BigQ c'tor: runlen: " << runlen << endl;

	cout << "1. BigQ c'tor: &in_pipe: " << in << endl;
	cout << "1. BigQ c'tor: &out_pipe: " << out << endl;
	cout << "1. BigQ c'tor: &myOrder: " << sortorder << endl;

	//in.Insert(NULL);
	//PipeOrders pipes = {&in, &out, &sortorder,runlen};

	PipeOrders pipes = {in, out, sortorder,runlen};


	cout << "2. BigQ() &in:  " << pipes.inPipe << endl;
	cout << "2. BigQ() &out: " << pipes.outPipe << endl;
	cout << "2. BigQ() &sortorder: " << pipes.order << endl;
	cout << "2. BigQ() runlen: " << pipes.runLength << endl;
	pthread_t thread1;
	pthread_create (&thread1, NULL, bigqthread, (void *)&pipes);
	//pthread_join (thread1, NULL);	
}*/

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	
	inPipe = &in;
	outPipe = &out;
	order = &sortorder;
	runLength = runlen;
	pthread_t worker_thread;
	
	if(pthread_create(&worker_thread, NULL, &bigqthread, (void *)this) != 0)
	{
		cerr<< "Worker thread creation failed"<<endl;
				exit(EXIT_FAILURE);
	}
	//pthread_join(worker_thread,NULL);
	
	outPipe->ShutDown ();
}

BigQ::~BigQ () {
}
