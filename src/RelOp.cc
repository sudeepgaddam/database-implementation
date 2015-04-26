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
		if (selOp == NULL){ //TODO - condition added to handle SelectFile - CNF = NULL; sampleQueries (3)
			passed++;
			outPipe->Insert(&rec);
		}
		else if (comp.Compare(&rec,literal, selOp)) {
			passed++;
			outPipe->Insert(&rec);
		}
	}
	//~ cout<<"selectfile_run() count: "<<count<<endl;
	//~ cout<<"selectfile_run() passed: "<<passed<<endl;

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
	BigQ b_queue(*(t->in),*(t->out),*(t->sort_order),t->run_len);
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
	util1->in=in1;
	util1->out=out1;
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
	bool first_time = true;
	while(out1->Remove(&rec)) {
		if (first_time) {
			//No Previous record to comapare with. Store current as prev_rec for next iteration
			prev_rec.Copy(&rec);
			first_time = false;
		} else {
			if (!comp.Compare(&rec, &prev_rec, &myOrder)) {
				//Prev Record same as current
				//Don't write to outPipe, No need to copy rec to prev_rec
			} else {	
				outPipe->Insert(&prev_rec);
				prev_rec.Copy(&rec);
			}
	}
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
	//~ cout<<"Buffer:"<<buffer<< "doublesum"<<doubleSum<<endl;
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

/*--------------------- Join Methods ----------------------------------*/

	void joinRecords(Record tmp, vector<Record> vec, Pipe *outPipe){

		if(vec.size()==0) return;

		int vrecAtts = vec[0].GetNumAtts();
		int tmpAtts  = tmp.GetNumAtts();
		int n = vrecAtts + tmpAtts;
		int* attsToKeep = new int [n];

		for(int i=0; i < vrecAtts + tmpAtts; i++){
			if(i < tmpAtts){
				attsToKeep[i] = i;
			} else{
				attsToKeep[i] = i - tmpAtts;
			}
		}

		for(int i=0; i<vec.size(); i++){
			Record vrec = vec[i];
			Record mergedRec;			
			mergedRec.MergeRecords(&tmp, &vrec, tmpAtts, vrecAtts, attsToKeep, vrecAtts + tmpAtts, tmpAtts);
			
			outPipe->Insert(&mergedRec);
		}

		delete attsToKeep;

	}

	void *join_run_old (void *arg) {
		Record Lrec,Rrec;
		ComparisonEngine comp;
		OrderMaker leftOrder, rightOrder;
		join_util *jutil = (join_util *)arg; 
		Pipe *inPipeL = jutil->inPipeL;
		Pipe *inPipeR = jutil->inPipeR;
		Pipe *outPipe = jutil->outPipe;
		CNF *selOp  = jutil->selOp;
		Record *literal = jutil->literal;
		int RunPages = jutil->RunPages;
		if (!selOp->GetSortOrders (leftOrder, rightOrder)) {
			//BNLJoin();
			return NULL;
		} 
		
		
		Pipe *in1;
    		Pipe *out1;
		bigq_util* util1;
		pthread_t left_bigq_thread;
		in1 = new  (std::nothrow) Pipe(PIPE_BUFFER);
		out1 = new (std::nothrow) Pipe(PIPE_BUFFER);
		util1 = new bigq_util();
		util1->in=in1;
		util1->out=out1;
		util1->sort_order=&leftOrder;
		util1->run_len=RunPages;
		pthread_create (&left_bigq_thread, NULL,run_bigq, (void*)util1);
		
		
		Pipe *in2;
    		Pipe *out2;
		bigq_util* util2;
		pthread_t right_bigq_thread;
		in2 = new  (std::nothrow) Pipe(PIPE_BUFFER);
		out2 = new (std::nothrow) Pipe(PIPE_BUFFER);
		util2 = new bigq_util();
		util2->in=in2;
		util2->out=out2;
		util2->sort_order=&rightOrder;
		util2->run_len=RunPages;
		pthread_create (&right_bigq_thread, NULL,run_bigq, (void*)util2);

		//~ cout << "RunPages: " << RunPages << endl;
		int countl=0, countr=0, recprocessed = 0;
		//Sort Merge Join
		while(inPipeL->Remove(&Lrec)) {
				countl++;//Lrec.Print(5);
				in1->Insert(&Lrec);
		}
		in1->ShutDown();
		//~ cout<<"Printed Left relation Records #count: " << countl <<endl;
		while(inPipeR->Remove(&Rrec)) {
				countr++;//Rrec.Print(3);
				in2->Insert(&Rrec);
		}
		in2->ShutDown();
		//~ cout<<"Printed Right relation Records #count: "<< countr <<endl;

		//
		bool out1Empty = false;
		bool out2Empty = false;
		if(!out1->Remove(&Lrec)) {
			out1Empty = true;
		}
		if(!out2->Remove(&Rrec)){
			out2Empty = true;
		}
		//Lrec.Print(5);
		//Rrec.Print(3);
		while( !out1Empty && !out2Empty) {
			recprocessed++;
			int compval = comp.Compare(&Lrec, &leftOrder, &Rrec, &rightOrder);
			//cout << "compval: " << compval << endl;
			if (comp.Compare(&Lrec, &leftOrder, &Rrec, &rightOrder) <0) {
				//left < right. GetNext from Left Pipe
				if(!out1->Remove(&Lrec)){
					out1Empty = true;
				}
				
			} else if (comp.Compare(&Lrec, &leftOrder, &Rrec, &rightOrder) >0){
				//left > right. GetNext from Right Pipe
				if(!out2->Remove(&Rrec)){
					out2Empty = true;
				}
			}else{
				//left = right
				Record tmpR;
				tmpR.Copy(&Rrec);
				vector<Record> rightRecsV;
				rightRecsV.push_back(Rrec);
				while(true){
					if(!out2Empty){
						 	if(out2->Remove(&Rrec)){
								if(!comp.Compare(&tmpR, &Rrec, &rightOrder)){
									//tmpR = Rrec
									rightRecsV.push_back(Rrec);
								}else{
									break;
								}
							}else{
								out2Empty = true;
							}
					}else{
						break;
					}
				}
				//cout << "Vector Size" <<rightRecsV.size() <<endl;
				Record tmpL;
				tmpL.Copy(&Lrec);
				joinRecords(tmpL, rightRecsV, outPipe);
				while(true) {
					if(!out1Empty){
							if(out1->Remove(&Lrec)){
								if(!comp.Compare(&tmpL, &Lrec, &leftOrder)){
									//Join Lrec with all Records in rightRecs
									joinRecords(tmpL, rightRecsV, outPipe);
								}else{
									break;
								}

							}else{
								out1Empty = true;
							}
					}else{
						break;
					}					
					
				}				
				rightRecsV.clear();
				/*if(!out1->Remove(&Lrec)) {
					out1Empty = true;
				}*/
				if(!out2->Remove(&Rrec)){
					out2Empty = true;
				}
				//cout << "First Operation done"  <<endl;
			}
			
		}
		//~ cout << "processed recs: " << recprocessed << endl;
		out1->ShutDown();
		out2->ShutDown();		
		outPipe->ShutDown();
	}

	/*void Join::Run_old (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { 
		
		join_util *jutil = new join_util();
		jutil->inPipeL = &inPipeL;
		jutil->inPipeR = &inPipeR;
		jutil->outPipe = &outPipe;
		jutil->selOp = &selOp;
		jutil->literal = &literal;
		jutil->RunPages = RunPages>0?RunPages:1;
		pthread_create (&thread, NULL, join_run, (void *)jutil);
			
	}*/
	
	void Join::WaitUntilDone () { 
		pthread_join (thread, NULL);
	}
	
	void Join::Use_n_Pages (int n) {
			RunPages = n;		
	}

/* -----------------------------------------------------------------------*/

/*--------------------- GroupBy Methods ----------------------------------*/

void *groupby_run (void *arg) {
	Record rec;
	Record temp;
	int intResult, intSum =0;
	double doubleResult, doubleSum = 0;
	Attribute sumAtt;
	ComparisonEngine comp;
	Record newRec, MergedRec;
	groupby_util *gbutil = (groupby_util *)arg; 
	Pipe *inPipe = gbutil->inPipe;
	Pipe *outPipe = gbutil->outPipe;
	OrderMaker *groupAtts = gbutil->groupAtts;
	Function *computeMe = gbutil->computeMe;
	char buffer[100];
	int AttsList [MAX_ANDS];

	groupAtts->GetAttsList(AttsList);
	
	int numAttsToKeep = groupAtts->GetNumAtts();
	int *NewAttsList = new int[100];
	//Fill the Atts list needed for the Merged Records
	for (int i=0; i< numAttsToKeep +1; i++) {
		if (i == 0) {
			NewAttsList[i] = 0;
		} else {
			NewAttsList[i] = AttsList[i-1];
		}	
	}
	
	
	int numAttsNow;
	sumAtt.name = strdup("SUM");
	if (inPipe->Remove(&rec)) {
		switch(computeMe->Apply(rec,intResult,doubleResult)) {
			
			case Int:
			  intSum = intResult;
			  sumAtt.myType = Int;
			break;
			case Double:
			  doubleSum = doubleResult;
			  sumAtt.myType = Double;
			break;
		
		}
		numAttsNow = rec.GetNumAtts();
		temp.Consume(&rec);
		//temp.Print(3);
		
	}
	
	Schema out_sch("sum_schema", 1, &sumAtt);
	
	while(inPipe->Remove(&rec)) {
		
		computeMe->Apply(rec,intResult,doubleResult);
		//If compare returns 0, Both the records are equal,
		//Compute the Sum on all the "Equal Records" Based on the groupAtts 
		if (!comp.Compare(&temp, &rec, groupAtts)) {
			if (sumAtt.myType == Int) {
				intSum+=intResult;
			} else if (sumAtt.myType == Double) {
				doubleSum += doubleResult;
			}
			
		} else {
			//Create a Record
			if (sumAtt.myType == Int) {
				//Write Int as string and compose Record
				sprintf (buffer, "%d|", intSum);
				
				intSum=intResult;
			} else if (sumAtt.myType == Double) {
				sprintf (buffer, "%f|", doubleSum);
				doubleSum = doubleResult;
			}
			newRec.ComposeRecord(&out_sch,(const char *) &buffer[0]);
			temp.Project (AttsList, numAttsToKeep, numAttsNow);
			MergedRec.MergeRecords (&newRec, &temp, 1, numAttsToKeep, NewAttsList, numAttsToKeep+1, 1);
			outPipe->Insert(&MergedRec);

		}
		temp.Consume(&rec);
	} //end of while loop
	/*
	 * 
	 * Todo -------------Put this in Function. This Doesn't look Good.
	 * Put last Grouped record also in the outpipe
	 */
		if (sumAtt.myType == Int) {
				//Write Int as string and compose Record
				sprintf (buffer, "%d|", intSum);
				
				intSum=intResult;
			} else if (sumAtt.myType == Double) {
				sprintf (buffer, "%f|", doubleSum);
				doubleSum = doubleResult;
			}
			newRec.ComposeRecord(&out_sch,(const char *) &buffer[0]);
			temp.Project (AttsList, numAttsToKeep, numAttsNow);
			MergedRec.MergeRecords (&newRec, &temp, 1, numAttsToKeep, NewAttsList, numAttsToKeep+1, 1);
			outPipe->Insert(&MergedRec);
			
			
		outPipe->ShutDown();

		free(sumAtt.name);
		//delete AttsList;
		delete NewAttsList;
}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
	
	groupby_util *gbutil = new groupby_util();
	gbutil->inPipe = &inPipe;
	gbutil->outPipe = &outPipe;
	gbutil->groupAtts = &groupAtts;
	gbutil->computeMe = &computeMe;
	
	pthread_create (&thread, NULL, groupby_run, (void *)gbutil);

}

void GroupBy::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void GroupBy::Use_n_Pages (int runlen) {

}



//-----------------Join-------------------------
//Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal
void* join_run(void* arg) {
	//cout<<"sss"<<endl;
	join_util* j_data = (join_util*) arg;

	Pipe *inPipeL = j_data->inPipeL;
	Pipe *inPipeR = j_data->inPipeR;
	Pipe *outPipe = j_data->outPipe;
	CNF *selOp = j_data->selOp;
	Record *literal = j_data->literal;
	int mem_pages = j_data->RunPages;

	//cout<<"dkdkd"<<endl;;
	OrderMaker left, right;
	int use_sort_merge;
	use_sort_merge = selOp->GetSortOrders(left, right);
	ComparisonEngine comp;
//	cout << "use merge " << use_sort_merge << endl;
	int join_calc_done = 0, numAttsLeft, numAttsRight, numAttsToKeep,
			startOfRight;
	int* attsToKeep;
	//Sort Merge Join
	//use_sort_merge=0;
	if (use_sort_merge) {
		Record rec1, rec2;
		//cout<<"sm1"<<endl;
		//inPipeL->Remove(&rec1);
		//inPipeR->Remove(&rec2);

		//cout<<"sm2"<<endl;
		Pipe* PipeL;
		Pipe* PipeR;
		PipeL = new (std::nothrow) Pipe(PIPE_BUFFER);
		PipeR = new (std::nothrow) Pipe(PIPE_BUFFER);
		//cout<<"sm3"<<endl;
		//pthread_t thread1,thread2;
		bigq_util* util1 = new (std::nothrow) bigq_util();
		util1->in = inPipeL;
		util1->out = PipeL;
		util1->run_len = mem_pages;
		util1->sort_order = &left;

		//cout<<"sm4"<<endl;
		bigq_util* util2 = new (std::nothrow) bigq_util();
		util2->in = inPipeR;
		util2->out = PipeR;
		util2->run_len = mem_pages;
		util2->sort_order = &right;

		pthread_t thread1, thread2;
		//cout<<"sm5"<<endl;
		pthread_create(&thread1, NULL, run_bigq, (void*) util1);

		pthread_create(&thread2, NULL, run_bigq, (void*) util2);


		int q1_status = PipeL->Remove(&rec1);
		int q2_status = PipeR->Remove(&rec2);
		//int q2_status;
		//cout<<"while true0"<<endl;
		int cnt = 0;

		Record mergedRecord;

		while (true) {
			//cout<<q1_status<<" "<<q2_status<<endl;
			if (q1_status && q2_status) {

				if (comp.Compare(&rec1, &left, &rec2, &right) < 0) {
					//cout<<"p1\n";
					q1_status = PipeL->Remove(&rec1);
					//cout<<cnt++<<endl;
					continue;

				} else if (comp.Compare(&rec1, &left, &rec2, &right) > 0) {
					//cout<<"p2\n";
					q2_status = PipeR->Remove(&rec2);
					continue;
				} else {
					//cout<<cnt++<<endl;

					//Compute cross product
					Record* temp = new (std::nothrow) Record();
					temp->Consume(&rec2);
					vector<Record*> rec_vector;

					while (q2_status
							&& !comp.Compare(&rec1, &left, temp, &right)) {
						//cout<<"2\n";
						//cout<<cnt++<<endl;
						rec_vector.push_back(temp);
						temp = new (std::nothrow) Record();
						q2_status = PipeR->Remove(temp);
					}

					if (q2_status)
						rec2.Consume(temp);

					delete temp;
					temp = NULL;
					//if(!rec1.bits) cout<<"1 culprit\n";
					//if(!rec_vector.front()->bits) cout<<"2 culprit\n";
					while (q1_status
							&& !comp.Compare(&rec1, &left, rec_vector.front(),
									&right)) {

						for (int i = 0; i < rec_vector.size(); i++) {
							if (comp.Compare(&rec1, rec_vector[i], literal,
									selOp)) {
								//cout<<"1"<<endl;
								if (!join_calc_done) {
									numAttsLeft = ((((int*) (rec1.bits))[1])
											/ sizeof(int)) - 1;
									numAttsRight =
											((((int*) (rec_vector[i]->bits))[1])
													/ sizeof(int)) - 1;
									numAttsToKeep = numAttsLeft + numAttsRight;

									attsToKeep =
											new (std::nothrow) int[numAttsToKeep];
									int k;
									for (k = 0; k < numAttsLeft; k++) {
										attsToKeep[k] = k;
									}

									startOfRight = k;

									for (int l = 0; l < numAttsRight;
											l++, k++) {
										attsToKeep[k] = l;
									}
									join_calc_done = 1;
								}

								mergedRecord.MergeRecords(&rec1, rec_vector[i],
										numAttsLeft, numAttsRight, attsToKeep,
										numAttsToKeep, startOfRight);
								outPipe->Insert(&mergedRecord);
								//cout<<"pipe written\n";

							}
						}
						q1_status = PipeL->Remove(&rec1);
						//cout<<cnt++<<endl;

					}

					for (int i = 0; i < rec_vector.size(); i++) {
						delete rec_vector[i];
					}

					rec_vector.empty();
				}
			}

			else {
				break;
			}
		}
		//cout<<"if done\n";
		delete PipeL;
		delete PipeR;
		delete attsToKeep;
		//return;
	} else {
		//Block Nested Loop Join

		Page p;
		int count = 0;
		vector<Record*> rec_vector1;
		vector<Record*> rec_vector2;
		int rec1_status = 0;
		int file_ready = 0;
		Record rec1, rec2;
		Record* temp;
		File f;
		f.Open(0, "temp.bin");
		int cur_page = 0;
		int scan_ptr = 0;
		int pipe2_status = 0;
//		cout << "mem_pages " << mem_pages << endl;
		//exit(1);
		int cnt = 0, cnt2 = 0;
		//while (inPipeL->Remove(&rec1)) {
		//	cout<<cnt++<<endl;
		//}
		//exit(1);
		while (true) {
			//cout<<"rec1_status: "<<rec1_status<<endl;
			//cout<<"count: "<<count<<endl;
			if ((rec1_status = inPipeL->Remove(&rec1))
					&& count < (mem_pages - 1)) {
				//cout<<"j1\n";
				//if(!rec1.bits) cout<<"fedrer\n";
//				cout << "cnt: " << cnt2++ << endl;
				//cout<<"rec1_status: "<<rec1_status<<endl;
				if (!p.Append(&rec1)) {
					//cout<<"j2\n";
					//cout<<"numrecs: "<<p.numRecs<<endl;
					temp = new (std::nothrow) Record();
					while (p.GetFirst(temp)) {
						//cout<<"cnt: "<<cnt++<<endl;
						//Schema s("catalog","supplier");
						//temp->Print(&s);
						//if(!temp->bits) cout<<"federer\n";
						rec_vector1.push_back(temp);
						temp = new Record();
						//if(!temp) cout<<"no mem cre"<<endl;
					}
					//if(!rec_vector1[0]->bits) cout<<"woah"<<endl;

					delete temp;
					temp = NULL;
					p.Append(&rec1);
					//p.EmptyItOut();
					count++;
//					cout << "count: " << count << endl;
				}

			} else {
				cnt += rec_vector1.size();
				//cout<<"recs: "<<cnt<<endl;
				//cout<<"rec1_status: "<<rec1_status<<endl;
//				if (file_ready)
//					cout << "file is ready\n";
				//cout<<"j3"<<endl;
				int scan_done = 0, scan_ptr = 0;
				Page scan_page;
				//cnt=0;

				if (count < mem_pages - 1) {
					//cout<<"in here\n";
					temp = new (std::nothrow) Record();
					while (p.GetFirst(temp)) {
						//if(!temp->bits)	cout<<"nadal"<<endl;
						rec_vector1.push_back(temp);
						temp = new (std::nothrow) Record();
					}
					delete temp;
					temp = NULL;
				}

				while (true) {
					if (!file_ready) {
						//First time when relation2 file not ready
						//int cnt=0;
						while ((pipe2_status = inPipeR->Remove(&rec2))
								&& scan_page.Append(&rec2)) {
							//cout<<cnt++<<endl;
							//cout<<"j4"<<endl;
						}

						//vector<Record*> temp_rec_vector;
						Page temp_page;
						Record* temp1;
						//cout<<"pip2 stat"<<pipe2_status<<endl;
						//cout<<"cnt: "<<cnt<<endl;
						temp = new (std::nothrow) Record();
						temp1 = new (std::nothrow) Record();
						while (scan_page.GetFirst(temp)) {
							temp1->Copy(temp);
							rec_vector2.push_back(temp);
							temp_page.Append(temp1);
							temp = new (std::nothrow) Record();
							temp1 = new (std::nothrow) Record();
						}
						delete temp;
						delete temp1;
						temp = NULL;
						temp1 = NULL;

						//cnt+=rec_vector2.size();
						//cout<<"temp: "<<cnt<<endl;
						f.AddPage(&temp_page, cur_page++);
						temp_page.EmptyItOut();
						scan_page.EmptyItOut(); //

					} else {
						//cout<<"flen: "<<f.GetLength()<<endl;
						//Scanning if relation2's file ready
						if (scan_ptr < f.GetLength() - 1) {
							f.GetPage(&scan_page, scan_ptr++);
							temp = new (std::nothrow) Record();
							while (scan_page.GetFirst(temp)) {
								rec_vector2.push_back(temp);
								temp = new (std::nothrow) Record();

							}
							delete temp;
							temp = NULL;
							scan_page.EmptyItOut();
							//cnt+=rec_vector2.size();
							//cout<<"temp: "<<cnt<<endl;
						} else
							scan_done = 1;
					}

					//cout<<"flen: "<<f.GetLength()<<endl;

//					else {
//						count=0;
//						p.Append(&rec1);
//					}

					cnt += rec_vector2.size();
					//cout<<"recs1: "<<rec_vector1.size()<<endl;
					//cout<<"recs2: "<<rec_vector2.size()<<endl;

					for (int i = 0; i < rec_vector1.size(); i++) {
						for (int j = 0; j < rec_vector2.size(); j++) {
//							if(!rec_vector1[i]->bits) {
//								cout<<"i: "<<i<<endl;
//								cout<<"culprit1"<<endl;
//							}
							//if(!rec_vector2[j]->bits) cout<<"culprit2"<<endl;
							if (comp.Compare(rec_vector1[i], rec_vector2[j],
									literal, selOp)) {
								//cout<<"j6"<<endl;
								Record mergedRecord;
								if (!join_calc_done) {
									numAttsLeft =
											((((int*) (rec_vector1[i]->bits))[1])
													/ sizeof(int)) - 1;
									numAttsRight =
											((((int*) (rec_vector2[j]->bits))[1])
													/ sizeof(int)) - 1;
									numAttsToKeep = numAttsLeft + numAttsRight;

									attsToKeep =
											new (std::nothrow) int[numAttsToKeep];
									int k;
									for (k = 0; k < numAttsLeft; k++) {
										attsToKeep[k] = k;
									}

									startOfRight = k;

									for (int l = 0; l < numAttsRight;
											l++, k++) {
										attsToKeep[k] = l;
									}
									join_calc_done = 1;
								}

								mergedRecord.MergeRecords(rec_vector1[i],
										rec_vector2[j], numAttsLeft,
										numAttsRight, attsToKeep, numAttsToKeep,
										startOfRight);
								outPipe->Insert(&mergedRecord);
								//cout<<"pipe written\n";

							}
						}
						//delete rec_vector1[i];
						//rec_vector1[i]=NULL;
					}

					for (int i = 0; i < rec_vector2.size(); i++) {
						delete rec_vector2[i];
						rec_vector2[i] = NULL;
					}

					//rec_vector1.clear();
					rec_vector2.clear();

					//cout<<"pipe2_status :"<<pipe2_status<<"file_ready: "<<file_ready<<endl;
					if (!pipe2_status && !file_ready) {
						file_ready = 1;
						break;
					} else if (!file_ready) {
//						temp= new (std::nothrow) Record();
//						temp->Consume(&rec2);
//						rec_vector2.push_back(temp);
//						temp=NULL;
						scan_page.Append(&rec2);
					}

					if (scan_done) {
						//scan_ptr=0;
						break;
					}

				}

				for (int i = 0; i < rec_vector1.size(); i++) {
					delete rec_vector1[i];
					rec_vector1[i] = NULL;
				}
				//cout<<"recs1: "<<rec_vector1.size()<<endl;
				//cout<<"recs2: "<<cnt<<endl;

				rec_vector1.clear();

				if (count >= (mem_pages - 1)) {
					p.Append(&rec1);
					cnt++;
					count = 0;

					//continue;
				} else
					break;

				//if(!rec1_status) break;

			}
			//cout<<"cnt2: "<<cnt2++<<endl;
		}

		f.Close(); //Tempfile close
		remove("temp.bin"); //Deleting the temp file
	}
	//delete PipeR;

	outPipe->ShutDown();

}


void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { 
		
		join_util *jutil = new join_util();
		jutil->inPipeL = &inPipeL;
		jutil->inPipeR = &inPipeR;
		jutil->outPipe = &outPipe;
		jutil->selOp = &selOp;
		jutil->literal = &literal;
		jutil->RunPages = RunPages>0?RunPages:1;
		pthread_create (&thread, NULL, join_run, (void *)jutil);
			
	}
