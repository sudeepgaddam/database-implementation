#include "BigQ.h"
#include <stdlib.h>
#include <vector>
#include <iostream>
#include "DBFile.h"


OrderMaker *localOrder;

int Compare (const void *a, const void *b){
	ComparisonEngine comp;
	return comp.Compare((Record *) a, (Record *)b, localOrder);
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
	//for(int i=0; i<1000; i++){
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
	

	// construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	out.ShutDown ();
}

BigQ::~BigQ () {
}
