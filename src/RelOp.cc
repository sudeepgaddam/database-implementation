#include "RelOp.h"
#include <string.h>

void *selectfile_run (void *arg) {
	Record rec;
	int count = 0;
	int passed = 0;
	ComparisonEngine comp;
	sfile_util *sfutil = (sfile_util *)arg; 
	DBFile *inFile = sfutil->inFile;
	Pipe *outPipe = sfutil->outpipe;
	CNF *selOp = sfutil->selOp;
	Record *literal = sfutil->literal;
	while (inFile->GetNext(rec)) {
		count++;
		if (comp.Compare(&rec,literal, selOp)) {
			passed++;
			outPipe->Insert(&rec);
		}
	}
	cout<<"selectfile_run() count: "<<count<<endl;
	cout<<"selectfile_run() passed: "<<passed<<endl;

	outPipe->ShutDown();

}
void *selectpipe_run (void *arg) {
	Record rec;

	ComparisonEngine comp;
	spipe_util *sputil = (spipe_util *)arg; 
	Pipe *inPipe = sputil->inpipe;
	Pipe *outPipe = sputil->outpipe;
	CNF *selOp = sputil->selOp;
	Record *literal = sputil->literal;
	while(inPipe->Remove(&rec)) {
		if (comp.Compare(&rec,literal, selOp)) {
			outPipe->Insert(&rec);
		}
	}
	outPipe->ShutDown();
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	sfile_util *sfutil = new sfile_util();
	sfutil->inFile = &inFile;
	sfutil->outpipe = &outPipe;
	sfutil->selOp = &selOp;
	sfutil->literal = &literal;
	pthread_create (&thread, NULL, selectfile_run, (void *)sfutil);

}

void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {

}
void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	spipe_util *sputil = new spipe_util();
	sputil->inpipe = &inPipe;
	sputil->outpipe = &outPipe;
	sputil->selOp = &selOp;
	sputil->literal = &literal;
	pthread_create (&thread, NULL, selectpipe_run, (void *)sputil);
}

void SelectPipe::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {

}

/* Project Methods */

void *project_run (void *arg) {
	Record rec;
	ComparisonEngine comp;
	proj_util *putil = (proj_util *)arg; 
	Pipe *inPipe = putil->inpipe;
	Pipe *outPipe = putil->outpipe;
	int *keepMe = putil->keepMe;
	int numAttsInput = putil->numAttsInput;
	int numAttsOutput = putil->numAttsOutput;
	
	while(inPipe->Remove(&rec)) {
		rec.Project(keepMe, numAttsOutput, numAttsInput); 
		outPipe->Insert(&rec);
	}

	outPipe->ShutDown();

}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
	proj_util *putil = new proj_util();
	putil->inpipe = &inPipe;
	putil->outpipe = &outPipe;
	putil->keepMe = keepMe;
	putil->numAttsInput = numAttsInput;
	putil->numAttsOutput = numAttsOutput;
	pthread_create (&thread, NULL, project_run, (void *)putil);

}

void Project::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void Project::Use_n_Pages (int runlen) {

}

/* DuplicateRemoval Methods */

void* run_bigq (void *arg) {
	
	bigq_util *t = (bigq_util *) arg;
	BigQ b_queue(*(t->inpipe),*(t->outpipe),*(t->sort_order),t->run_len);
}


void *duprem_run (void *arg) {
	Record rec;
	Record prev_rec;
	ComparisonEngine comp;
	duprem_util *dr_util = (duprem_util *)arg; 
	Pipe *inPipe = dr_util->inpipe;
	Pipe *outPipe = dr_util->outpipe;
	Schema *mySchema = dr_util->mySchema;
	
	OrderMaker myOrder(mySchema);
	
	Pipe *in1;
    Pipe *out1;
	bigq_util* util1;
	pthread_t bigq_thread;
	in1 = new  (std::nothrow) Pipe(PIPE_BUFFER);
	out1 = new (std::nothrow) Pipe(PIPE_BUFFER);
	util1 = new bigq_util();
	util1->inpipe=in1;
	util1->outpipe=out1;
	util1->sort_order=&myOrder;
	// Hard coding to 1 for now
	util1->run_len=1;
	pthread_create (&bigq_thread, NULL,run_bigq, (void*)util1);
		

	//Write all the records to BigQ first
	//Get the records from outPipe and then compare with the previously fetched records
	//If it is same, then don't write that record to outPipe
	while(inPipe->Remove(&rec)) {
		in1->Insert(&rec);
	}
	
	in1->ShutDown();
	
	while(out1->Remove(&rec)) {
		outPipe->Insert(&rec);
	}
	outPipe->ShutDown();
	pthread_join(bigq_thread, NULL);
	
}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
	duprem_util *dr_util = new duprem_util();
	dr_util->inpipe = &inPipe;
	dr_util->outpipe = &outPipe;
	dr_util->mySchema = &mySchema;

	pthread_create (&thread, NULL, duprem_run, (void *)dr_util);

}

void DuplicateRemoval::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void DuplicateRemoval::Use_n_Pages (int runlen) {

}
/*--------------------- SUM Methods --------------------------------------*/

void *sum_run (void *arg) {
	Record rec;
	ComparisonEngine comp;
	int intResult, intSum =0;
	double doubleResult, doubleSum = 0;
	char buffer[100];
	sum_util *sutil = (sum_util *)arg; 
	Pipe *inPipe = sutil->inpipe;
	Pipe *outPipe = sutil->outpipe;
	Function *computeMe = sutil->computeMe;
	Attribute sumAtt;
	
	//First Remove a record. Apply Function and find out its type, 
	//Fill the Attribute accordingly. Then, compute the sum on all the records
	// Construct a schema for just one attribute. Call composeRecord.
	if (inPipe->Remove(&rec)) {
		sumAtt.name = strdup("SUM");
		switch(computeMe->Apply(rec,intResult,doubleResult)) {
			case Int:
			  intSum = intResult;
			  sumAtt.myType = Int;
			  //sumAtt.name = "int";
			  cout<<"Its int"<<endl;
			break;
			case Double:
			  doubleSum = doubleResult;
			  sumAtt.myType = Double;
			  //sumAtt.name = "double";
			  cout<<"Its double  - dobleResult:"<< doubleResult<<endl;


			break;
	}
	
	
	while(inPipe->Remove(&rec)) {
		//If it returns int, then use intResult
		if (computeMe->Apply(rec,intResult,doubleResult) == Int) {
			intSum+=intResult;
		} else {
			//Use doubleResult
			doubleSum+=doubleResult;
		}
	}
	
	Schema out_sch("sum_schema", 1, &sumAtt);
	if (sumAtt.myType == Int) {
		sprintf (buffer, "%d|", intSum);
	} else {
		sprintf (buffer, "%f|", doubleSum);
	}
	cout<<"Buffer:"<<buffer<< "doublesum"<<doubleSum<<endl;
	Record newRec;
	newRec.ComposeRecord(&out_sch,(const char *) &buffer[0]);
	//newRec.Print(&out_sch);
	outPipe->Insert(&newRec);
	outPipe->ShutDown();
	free(sumAtt.name);
	//if it is a record
}
}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
	sum_util *sutil = new sum_util();
	sutil->inpipe = &inPipe;
	sutil->outpipe = &outPipe;
	sutil->computeMe = &computeMe;

	pthread_create (&thread, NULL, sum_run, (void *)sutil);

}

void Sum::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void Sum::Use_n_Pages (int runlen) {

}
/* -----------------------------------------------------------------------*/

/*--------------------- WriteOut Methods ----------------------------------*/

void *write_run (void *arg) {
	Record rec;
	ComparisonEngine comp;
	write_util *wutil = (write_util *)arg; 
	Pipe *inPipe = wutil->inpipe;
	FILE *outFile = wutil->outFile;
	Schema *mySchema = wutil->mySchema;
	while(inPipe->Remove(&rec)) {
		rec.FilePrint(outFile, mySchema); 
	}
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
	write_util *wutil = new write_util();
	wutil->inpipe = &inPipe;
	wutil->outFile = outFile;
	wutil->mySchema = &mySchema;

	pthread_create (&thread, NULL, write_run, (void *)wutil);

}

void WriteOut::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void WriteOut::Use_n_Pages (int runlen) {

}
