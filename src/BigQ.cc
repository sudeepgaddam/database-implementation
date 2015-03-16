#include "BigQ.h"
#include <stdlib.h>
#include <vector>
#include <iostream>
#include "DBFile.h"
#include <queue>
#include <stdio.h>

int BigQ::filecounter = 0;

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


void phasetwo(int num_runs, int runlen, DBFile* infile, Pipe *outpipe, int filecounter){
	

	int num_buffers = num_runs; // num of runs	
	vector <Record> outbuffer;
	int outpointer;
	vector<Page> buffers;

	priority_queue<RecordInfo> pq;  //greater than comparison -> min pq

	char *tmppath0 = "abc0";
	char *tmppath1 = "abc1";
	
	char * fpath;
	cout << "phasetwo-filecounter: " << filecounter << endl;
	if(filecounter==1) fpath = tmppath0;
	else fpath = tmppath1;
	//infile->Open(fpath);
	infile->MoveFirst();

	cout << "phasetwo-fpath: " << fpath << endl;

	off_t whichpages[num_buffers];

	int outpiperecs = 0;
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
		cout << "mpage recs: " << m_page->GetNumRecs() << endl;
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

	char *fout0 = "out0";
	char *fout1 = "out1";
	
	char * foutpath;
	if(filecounter==1) foutpath = fout0;
	else foutpath = fout1;

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
				outpiperecs++;
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
		//cout << "pq.size(): " << pq.size() << endl;
		if(pq.size()==1){
			one_buffer_left = 1;
			//flush output buffer records even if it is not full - since we are done with all runs
			for(int i=0; i<outbuffer.size();i++){
				//outfile->Add(outbuffer[i]);
				outpiperecs++;
				outpipe->Insert(&outbuffer[i]);
			}
			outbuffer.clear();
		}
	}

	cout << "***** Processing last sorted run one_buffer_left: " << one_buffer_left << endl;

	if(one_buffer_left){
		//flush priority que record into out file		
		RecordInfo tempInfo = pq.top();
		pq.pop();
		int bufferindex = tempInfo.bufferId;
		//tempInfo.rec->Print(schema);
		//outfile->Add(*tempInfo.rec);
		outpiperecs++;
		outpipe->Insert(tempInfo.rec);
		//flush all buffer records into outfile		
		while(true){
			tempInfo.rec = new Record();
			if(buffers[bufferindex].GetFirst(tempInfo.rec)){	//replaced GetCurrent	//need to break into small functions !!
				//cout << "Add buffer record into file" << endl;
				//tempInfo.rec->Print(schema);
				//outfile->Add(*tempInfo.rec);
				outpiperecs++;
				outpipe->Insert(tempInfo.rec);
			}else{
				cout << "End of last buffer!!" << endl;
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
				//cout << "Dump records into outpipe" << endl;
				outpiperecs++;
				outpipe->Insert(rec);
			}
			whichpages[bufferindex]++;
		}

	}
	cout << "***** Success!! Phase Two Ends outpiperecs: " << outpiperecs << endl;

	infile->Close();
	remove(foutpath);


}



BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	int count = 0;
	
	filecounter++;

	// read data from in pipe sort them into runlen pages
	Record rec;
	Record temp;
	localOrder = &sortorder;
	Schema *rschema = new Schema ("data/catalog", "region");
	Schema *lschema = new Schema ("data/catalog", "lineitem");
	Schema *psschema = new Schema ("data/catalog", "partsupp");
	Schema *pschema = new Schema ("data/catalog", "partsupp");
    	Schema *schema = pschema;
	DBFile tempFile;

	char *tmppath0 = "abc0";
	char *tmppath1 = "abc1";
	
	char * fpath;
	if(filecounter==1) fpath = tmppath0;
	else fpath = tmppath1;

	cout << "BigQ.filecounter: " << filecounter << endl;
	cout << "BigQ.fpath: " << fpath << endl;

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

	int incounter = 0;
	int counter = 0;
	
	while(in.Remove(&rec)){

		incounter++;
		
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
				counter++;
				tempFile.Add(vrec[i]);
			}
			vrec.clear();
			size = recsize;
			vrec.push_back(rec);
			recordcount = 1;
		}
	}
	
	cout << "finished reading: recs - " << incounter << endl;
	cout << "finished reading: runs - " << runcount << endl;
				
	int vsize = recordcount;//vrec.size();
	if(vsize >0) { 
		runcount++;
		//cout << "start qsort" << endl; 
		qsort((void *) &vrec[0], recordcount, sizeof(Record), Compare);
	    	//cout << "end qsort" << endl;
	}

	for(int i=0; i<vsize; i++){
		counter++;
        	tempFile.Add(vrec[i]);
	}
	vrec.clear();
	//int tempFile_currlen = tempFile.Close();

	cout << "***** Success!! Phase one ends with runs=qsortcount: " << runcount << endl;
	cout << "***** Infile Records: " << counter << endl;
	//cout << "***** Infile curlen: " << tempFile_currlen << endl;

	phasetwo(runcount, runlen, &tempFile, &out, filecounter);
	

	// construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    	// finally shut down the out pipe
	out.ShutDown ();
	cout << "Bigq() out pipe addess: "<<&out <<endl;
	cout << "=============== end  TPMMS ===============" << endl;
	cout << endl;
	cout << endl;
	remove(fpath);
	
	
	
	
}

BigQ::~BigQ () {
}
