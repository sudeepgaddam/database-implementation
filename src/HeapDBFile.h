#ifndef HEAPDBFILE_H
#define HEAPDBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"

class HeapDBFile : virtual public GenericDBFile{

private:
    File *heapfile;
    Page *write_page; //Record writes go into this page
    Page *read_page;  //This page is only for reading
    int cur_page;     //Current Page being read. 0 means no pages to read
    bool dirty;       //If true, current page being read is dirty(Not yet written to disk). 
    fType type;

public:
    //Constructor
	HeapDBFile ();
    //Destructor
    ~HeapDBFile (); 
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
	void FlushWritePage();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

	//page level read and wirte
	int GetPage (Page *putItHere, off_t whichPage);
	//void AddPage(Page *srcPage);

	bool isEmpty();
	void DumpWriteBuffer();
};
#endif


