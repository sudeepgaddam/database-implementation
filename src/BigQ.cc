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


class CompareRecordInfo{
	public:
		bool operator()(RecordInfo& rInfoa, RecordInfo& rInfob){
			cout << "Inside CompareRecordInfo class" << endl;
			Record *reca = (rInfoa.rec);
			Record *recb = (rInfob.rec);
			int ret = compEng.Compare(reca, recb, localOrder);
			if(ret <0) return true;
			else return false;
		}	
};

void phasetwo(int num_runs, int runlen, DBFile* infile){
	
	Schema *schema = new Schema ("data/catalog", "lineitem");
	int num_buffers = num_runs; // num of runs
	
	vector <Record> outbuffer;
	int outpointer;
	vector<Page> buffers;
	//int pointers[num_buffers];

	priority_queue<RecordInfo> pq;  //greater than comparison -> min pq
	
	char * fpath = "lineitem.in";
	infile->Open(fpath);
	infile->MoveFirst();

	off_t whichpages[num_buffers];

	cout << "start insertion into pq" << endl;
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

		if(!buffers[i].GetCurrent(recInfo.rec)) {
			cout << "unable to read current record" << endl;			
			continue;
		}

		recInfo.rec->Print(schema);		 
		recInfo.bufferId = i;		
		pq.push(recInfo);	
		
		delete m_page;
		                
	}

	cout << "insertion into pq sucess" << endl;
	/*while(!pq.empty()){
		RecordInfo currentRecInfo = pq.top();
		pq.pop();
		Record* current  = currentRecInfo.rec;
		current->Print(schema);
	}*/
	
	
	;
	char * foutpath = "lineitems.sorted";
	DBFile* outfile = new DBFile();
	outfile->Create(foutpath, heap, NULL);
	outfile->Open(foutpath);

	cout << "TPMMS logic start" << endl;
	
	int outbuffersize = 0;
	int one_buffer_left = 0;
	outpointer = -1;

	/*while(!pq.empty() && !one_buffer_left){
		RecordInfo currentRecInfo = pq.top();
		pq.pop();
		Record* current  = currentRecInfo.rec;
		int bufferindex = currentRecInfo.bufferId;
		cout << "least record" << endl;
		//current->Print();
		outbuffer.push_back(*current);
		outbuffersize += current->GetSize();
		outpointer++;
		//if output buffer is full write into file
		if(outbuffersize==PAGE_SIZE){ 
			for(int i=0; i<outbuffer.size();i++){
				outfile->Add(outbuffer[i]);	
			}
			outbuffersize = 0;
			outbuffer.clear();
			outpointer = -1;
		}		

		RecordInfo tempInfo;		
		tempInfo.bufferId = bufferindex;
		tempInfo.rec = new Record();
		if(!buffers[bufferindex].GetCurrent(tempInfo.rec)) {
			whichpages[bufferindex]++;
			cout << "reached end of page, so read next page into buffer" << endl;			
			if(!infile->GetPage(&buffers[bufferindex], whichpages[bufferindex])){
				cout << "ERROR: Not able to read page" << endl;
				//continue;
			}else{
				buffers[bufferindex].MoveToStart();
				if(buffers[bufferindex].GetCurrent(tempInfo.rec))
					pq.push(tempInfo);
			}
		}else{
			//increment pointer to next record
			cout << "push next record" << endl;
			pq.push(tempInfo);
		}
		
		if(pq.size()==1)
			one_buffer_left = 1;	
	}

	cout << "PROCESSING LAST SORTED RUN" << endl;

	if(one_buffer_left){
		RecordInfo tempInfo = pq.top();
		pq.pop();
		int bufferindex = tempInfo.bufferId;
		outfile->Add(*tempInfo.rec);
		tempInfo.rec = new Record();
		while(buffers[bufferindex].GetCurrent(tempInfo.rec)){		//need to break into small functions !!
			outfile->Add(*tempInfo.rec);
		}
		Page p;
		whichpages[bufferindex]++;
		while(infile->GetPage(&buffers[bufferindex], whichpages[bufferindex]))	{	
			buffers[bufferindex].MoveToStart();
			Record *rec = new Record();
			while(buffers[bufferindex].GetCurrent(rec)){
				outfile->Add(*rec);
			}
			whichpages[bufferindex]++;
		}

	}*/
	cout << "PHASE TWO ENDS" << endl;

}



BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	
	localOrder = &sortorder;
	int count = 0;

	// read data from in pipe sort them into runlen pages
	Record rec;
	Record temp;
    	Schema *schema = new Schema ("data/catalog", "lineitem");
	DBFile tempFile;
	char * fpath = "lineitem.in";
	tempFile.Create(fpath, heap, NULL);

      	vector<Record> vrec;	
	int runcount = 0;
	int size    = 0;
	int recordcount = 0;	
	tempFile.Open(fpath);

	cout << "Start reading from in pipe" << endl;
	int i = 0;
	
	while(in.Remove(&rec)){
		
		recordcount++;
		int recsize = rec.GetSize();
		size += recsize;
		if(size <= PAGE_SIZE*runlen) {
			vrec.push_back(rec); 
		}
		else{
			runcount++;
			cout << "One Run Completed. Run count" << runcount<<endl;
			
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
	
	//cout << "finished reading: recs - " << recordcount << endl;
	//cout << "finished reading: runs - " << runcount << endl;
				
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

	cout << "Success!! PHASE ONE ended with runs=qsortcount: " << runcount << endl;

	phasetwo(runcount, runlen, &tempFile);
	

	// construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	out.ShutDown ();
}

BigQ::~BigQ () {
}
