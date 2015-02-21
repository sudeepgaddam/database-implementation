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
	Record rec;
	int bufferId;
	

	bool operator < (const RecordInfo& rInf) const{
		const Record *reca = &rec;
		const Record *recb = &(rInf.rec);
		
		return Compare((void *) reca,(void *) recb) > 0;
	}
};


void phasetwo(int num_runs, int runlen, DBFile* dfile){
	
	Schema *schema = new Schema ("data/catalog", "lineitem");
	int num_buffers = num_runs; // num of runs
	
	vector <Record> outbuffer;
	int outpointer;
	vector<Page> buffers;
	int pointers[num_buffers];

	priority_queue<RecordInfo, vector<RecordInfo> > pq;  //greater than comparison -> min pq

	
	char * fpath = "lineitem.in";
	dfile->Open(fpath);
	off_t whichPage = 1;

	cout << "start insertion into pq" << endl;
	for(int i=0; i<num_buffers; i++){

		// read one page into each buffer
		Page m_page;
		if(!dfile->GetPage(&m_page, whichPage)){
			cout << "ERROR: Not able to read page" << endl;
			continue;
		}
		if(m_page.GetNumRecs() <=0) continue;
		cout << "read- " << whichPage << " -Page" << endl;
		buffers.push_back(m_page); 	
		cout << "read- " << buffers[i].GetNumRecs() << " -Records" << endl;	
		whichPage += runlen;
		
		
		// insert current recInfos into pq --TODO boundary conditions			  
		RecordInfo recInfo;
		buffers[i].MoveToStart (); //since getpage advances current to end
		if(!buffers[i].GetCurrent(&recInfo.rec)) {
			cout << "read -current record" << endl;			
			continue;
		}
		cout << "printing record" << endl;
		recInfo.rec.Print(schema);		 
		recInfo.bufferId = i;
		pq.push(recInfo);
		cout << "pushed recInfo into pq" << endl;
		
		// initialize pointers         	
		pointers[i] =0;                 
	}

	cout << "insertion into pq sucess" << endl;
	while(!pq.empty()){
		RecordInfo currentRecInfo = pq.top();
		pq.pop();
		Record current  = currentRecInfo.rec;
		current.Print(schema);
	}

	

	/*int one_buffer_left = 0;
	while(!pq.empty() && !one_buffer_left){
		RecordInfo currentRecInfo = pq.top();
		Record current  = currentRecInfo.rec;
		int bufferindex = currentRecInfo.bufferid;
		outbuffer.push_back(current);
		outpointer++;
		if(outbuffer.full()){
			diskwrite(tempfile, outbuffer);
			outpointer = 0;
		}		

	
		//int bufferindex = map.find(current);
		pointers[bufferindex]++;
		//map.erase(current);
		if(pointers[bufferindex] >PAGE_SIZE){
			diskread(tempfile, buffers[bufferindex]);
			pointers[bufferindex] = 0;
		}
		RecordInfo tempInfo;		
		tempInfo.rec = buffers[pointers[bufferindex]];
		tempInfo.bufferId = bufferindex;
		pq.add(tempInfo);
		//map.put(temp, bufferindex);

		if(pq.size()==1)
			one_buffer_left = 1;	
	}

	if(one_buffer_left){
		RecordInfo tempInfo = pq.top();
		Record temp = tempInfo.rec;
		int bufferindex = tempInfo.bufferId; //map.find(temp);
		for(int i=pointers[bufferindex]; i<PAGE_SIZE; i++)
			tempfile.addRecord(temp);
		Page p;
		while(diskread(tempfile, p))		
			tempfile.addPage(p);

	}*/

}



BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	
	localOrder = &sortorder;
	int count = 0;
	// read data from in pipe sort them into runlen pages
	Record rec;
	Record temp;
    	Schema *schema = new Schema ("data/catalog", "lineitem");
	DBFile tempFile;
	cout << "Before creating DBfile" << endl;
	char * fpath = "lineitem.in";
	tempFile.Create(fpath, heap, NULL);

      	vector<Record> vrec;	
	int runcount = 0;
	int size    = 0;
	int recordcount = 0;
	cout << "Before opening DBfile" << endl;	
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
	
	cout << "finished reading: recs - " << recordcount << endl;
	cout << "finished reading: runs - " << runcount << endl;
				
	int vsize = recordcount;//vrec.size();
	if(vsize >0) { 
		runcount++;
		cout << "start qsort" << endl; 
		qsort((void *) &vrec[0], recordcount, sizeof(Record), Compare);
	    cout << "end qsort" << endl;
	}
	for(int i=0; i<vsize; i++){
        	tempFile.Add(vrec[i]);
	}
	while (tempFile.GetNext (temp) == 1) {
		count++;
		if (count%1000 == 0) {
		temp.Print(schema);
	}
	}
	vrec.clear();
	tempFile.Close();

	cout << "Success!" << endl;

	phasetwo(runcount, runlen, &tempFile);
	

	// construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	out.ShutDown ();
}

BigQ::~BigQ () {
}
