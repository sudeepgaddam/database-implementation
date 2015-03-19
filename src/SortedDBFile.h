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

typedef enum {Read, Write} Mode;

typedef struct SortInfo {
	OrderMaker *myOrder;
	int runLength;
}SortInfoDef; 

class SortedDBFile : virtual public GenericDBFile {

private:
	Page *outpipe_write_page;
	char* fpath;
	File *outpipefile; //sortedfile
    File *heapfile;
    Page *write_page; //Record writes go into this page
    Page *read_page;  //This page is only for reading
    int cur_page;     //Current Page being read. 0 means no pages to read
    bool dirty;       //If true, current page being read is dirty(Not yet written to disk). 
    fType type;

	Mode mode;

	Pipe *in;
    	Pipe *out;
    	OrderMaker *myOrder;
	int runLength;
	HeapDBFile *heapDB;
	bigq_util* util;
	pthread_t thread1;

		

public:

	static int addcount;

    //Constructor
	SortedDBFile ();
    //Destructor
    ~SortedDBFile (); 
    /*
     * Creates the file using fpath and file type can be heap, tree, sorted
     * Need to store the file information in metafile *.header
     * @return 1 : success
     *         0 : error
     */
	int Create (char *fpath, fType file_type, void *startup);
    /*
     * Open the file given by fpath 
     * @return 1 : success
     *         0 : error
     */
	int Open (char *fpath);
    /*
     * close the opened file. 
     * @return 1 : success
     *         0 : error
     */
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	/* Given an out stream reference to meta datafile
	 * Get ordermaker and runlength
     	 * @return 1 : success
     	 *         0 : error
     	 */
	int GetFromMetaData (ifstream &ifs);

	//page level read and wirte
	int GetPage (Page *putItHere, off_t whichPage);
	//void AddPage(Page *srcPage)

	void SwitchMode();

	void Merge();

	int AddToOutPipeFile(Record &rec);	

	void FlushWritePage();

	
};

void* producer (void *arg);
#endif
