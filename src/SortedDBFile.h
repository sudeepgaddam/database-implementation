#ifndef SORTDBFILE_H
#define SORTDBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Pipe.h"
#include "BigQ.h"
#include "GenericDBFile.h"
#include "HeapDBFile.h"

#define PIPE_BUFFER 100

typedef enum {reading, writing} file_mode;

typedef struct SortInfo {
 OrderMaker *myOrder;
 int runLength;
} Sort_Info;

class SortedDBFile : virtual public GenericDBFile {


private:
	char* fpath;
	File data_file;
	File temp_data_file;
	ofstream metadata_file;
	Page buffer_page;
	Page temp_buffer_page;
	off_t cur_page;
	off_t temp_buffer_ptr;
	OrderMaker* sort_info;
	Pipe* in;
	Pipe* out;
	file_mode mode;
	//BigQ b_queue;
	int runlen;
	//int count=0;
	int search_done;
	bigq_util* util;
	pthread_t thread1;

public:
	SortedDBFile ();
	~SortedDBFile ();
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();
	void Load (Schema &f_schema, char *loadpath);
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	void SwitchMode();
	//void RunBigQ(bigq_util*);
	void Merge_with_q();
	void Add_New_File(Record &addme);
	int GetNext_File(Record &fetchme);
	int GetFirst_Match(Record &fetchme, Record &literal, CNF &cnf, OrderMaker &search_order,
			OrderMaker &literal_order);
	int GetNext_NO_BS(Record &fetchme, CNF &cnf, Record &literal);
	int GetNext_With_BS(Record &fetchme, CNF &cnf, Record &literal,OrderMaker &order,OrderMaker &literal_order);
};
#endif
