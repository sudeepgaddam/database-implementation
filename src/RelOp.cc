#include "RelOp.h"

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
		rec.Print();
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
