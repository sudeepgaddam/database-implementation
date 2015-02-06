#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;

// stub DBFile header..replace it with your own DBFile.h 

class DBFile {

private:
    File *heapfile;
    Page *write_page; //Record writes go into this page
    Page *read_page; //This page is only for reading
    int cur_page;
    fType type;

public:
    //Constructor
	DBFile ();
    //Destructor
    ~DBFile (); 
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
	void Add (Record *addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif

