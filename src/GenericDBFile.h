#ifndef GENERICDBFILE_H
#define GENERICDBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"


typedef enum {heap, sorted, tree} fType;

class GenericDBFile {

public:
    //Constructor
	GenericDBFile ();
    //Destructor
    ~GenericDBFile (); 
    /*
     * Creates the file using fpath and file type can be heap, tree, sorted
     * Need to store the file information in metafile *.header
     * @return 1 : success
     *         0 : error
     */
virtual	int Create (char *fpath, fType file_type, void *startup);
    /*
     * Open the file given by fpath 
     * @return 1 : success
     *         0 : error
     */
virtual	int Open (char *fpath);
    /*
     * close the opened file. 
     * @return 1 : success
     *         0 : error
     */
virtual	int Close ();

virtual	void Load (Schema &myschema, char *loadpath);

virtual	void MoveFirst ();
virtual	void Add (Record &addme);
virtual	int GetNext (Record &fetchme);
virtual	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

	//page level read and wirte
virtual	int GetPage (Page *putItHere, off_t whichPage);
	//void AddPage(Page *srcPage);

virtual void DumpWriteBuffer();

virtual void FlushWritePage();
	
};
#endif

