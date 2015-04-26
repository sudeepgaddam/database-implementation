#include "BigQ.h"
#include <stdlib.h>
//#include <pthread.h>
using namespace std;

//pthread_t pthread_self();


OrderMaker sort_order;

class Sort_Run{

OrderMaker *sort_order;
public:

	Sort_Run(OrderMaker *abc)
	{
		sort_order = abc;
	}

bool operator()(Record* r1,Record* r2) {
	ComparisonEngine comp;
	if(comp.Compare(r1,r2,sort_order)<0)
		return true;
	else return false;
}
};

class Sort_Merge{

	OrderMaker *sort_order;
public:

	Sort_Merge(OrderMaker *abc)
	{
		sort_order = abc;
	}

bool operator()(Record* r1,Record* r2) {
	ComparisonEngine comp;
	if(comp.Compare(r1,r2,sort_order)<0) return false;
	else return true;
}
};

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	if(runlen<=0) {

		out.ShutDown();
		return;


	}
	OrderMaker* sort_order=&sortorder;
	map<int,Page*> overflow;
	int count=0;
	int page_count=0;
	Record rec;
	Record* temp;
	File f;
	Page* p=new (std::nothrow) Page();
	Page* file_page;
	vector<Record*> rec_vector;
	vector<Page*> page_vector;
	//gettid();
	//cout<<rand()<<endl;
	char temp_file[100];
	//cout<<temp_file<<endl;
	sprintf(temp_file,"temp%d.bin",rand());
	f.Open(0,temp_file); //Tempfile for maintaining sorted run
	//cout<<"cbble\n";
	while(true) {

		if(in.Remove(&rec) && count<runlen) {
			//cout<<"removing: "<<temp_file<<endl;
			//Schema s("catalog","lineitem");

			if(!p->Append(&rec)) {
				page_vector.push_back(p);
				p=new (std::nothrow) Page();
				p->Append(&rec);
				count++;
			}
		}

		else { //Got runlen pages, now sort them

			//Incase of last run, push the large page for sorting
			if(count<runlen && p->GetNumRecs()) {
				page_vector.push_back(p);
			}

			//Extract records of each run from pages, do an in-memory sort
			for(int j=0;j<page_vector.size();j++) {
				temp=new (std::nothrow) Record();
				while(page_vector[j]->GetFirst(temp)) {
					rec_vector.push_back(temp);
					temp=new (std::nothrow) Record();
				}
				delete temp;
				delete  page_vector[j];
				page_vector[j]=NULL;
				temp=NULL;
			}

			//Using in-built algorithm for sort
			std::sort(rec_vector.begin(),rec_vector.end(),Sort_Run(&sortorder));

			//Writing each run into temp file
			int page_count1=0;
			file_page=new (std::nothrow)Page();
			for(int i=0;i<rec_vector.size();i++) {
				if(!file_page->Append(rec_vector[i])) {
					page_count1++;
					f.AddPage(file_page,page_count++);
					file_page->EmptyItOut();
					file_page->Append(rec_vector[i]);
				}
				delete rec_vector[i];
				rec_vector[i]=NULL;
			}

			if(page_count1<runlen) {
				if(file_page->GetNumRecs())
					f.AddPage(file_page,page_count++);
			}
			else { //Check for overflow page and map the run number to it 
				if(file_page->GetNumRecs()) {

					overflow[page_count-1]=file_page;
				}
			}
			file_page=NULL;

			rec_vector.clear();
			page_vector.clear();

			//Appending the record of next-run into buffer page, and setting run-page count to 0
			if (count>=runlen) {
				p->Append(&rec);
				count=0;
				continue;
			}
			else {
				break;
			}
		}
	}

	//Parameters for merge step
	off_t file_length=f.GetLength();
	off_t no_of_runs;
	if(file_length!=0)
		 no_of_runs=((ceil)((float)(file_length-1)/runlen));
	else no_of_runs=0;
	off_t last_run_pages=(file_length-1)-(no_of_runs-1)*runlen;

	priority_queue<Record*,vector<Record*>,Sort_Merge> p_queue(&sortorder);

	//Maintains the pointer to rec_head
	//Record** rec_head=new (std::nothrow) Record*[no_of_runs];

	//Maps Record to run (for k-way merge)
	map<Record*,int> m_record;

	//Maintains page numbers of each run
	int* page_index=new (std::nothrow) int[no_of_runs];

	//Maintains current buffer page in each run
	Page** page_array=new (std::nothrow) Page*[no_of_runs];

	//Load first pages into the memory buffer and first record into priority queue
	int page_num=0;

	for(int i=0;i<no_of_runs;i++) {
		page_array[i]=new Page();
		f.GetPage(page_array[i],page_num);
		page_index[i]=1;
		Record* r=new (std::nothrow) Record;
		page_array[i]->GetFirst(r);

		p_queue.push(r);
		//rec_head[i]=r;
		m_record[r]=i;
		r=NULL;
		page_num+=runlen;
	}

	//Extract from priority queue and place on the output pipe
	while(!p_queue.empty()) {

		Record* r=p_queue.top(); //Extract the min priority record
		p_queue.pop();

		//run number of record being pushed, -1 sentinel
		int next_rec_run=-1;

		next_rec_run=m_record[r];
		m_record.erase(r);

		if(next_rec_run == -1) {
			cout<<"Weird! Since element which was mapped before isn't present"<<endl;
			return;
		}

		Record* next=new (std::nothrow) Record;
		bool is_rec_found=true;
		if(!page_array[next_rec_run]->GetFirst(next)) {
			//Check if not the end of run 
 			if((!(next_rec_run==(no_of_runs-1)) && page_index[next_rec_run]<runlen)
					|| ((next_rec_run==(no_of_runs-1) && page_index[next_rec_run]<last_run_pages))){
				
				f.GetPage(page_array[next_rec_run],page_index[next_rec_run]+next_rec_run*runlen);
				page_array[next_rec_run]->GetFirst(next);
				page_index[next_rec_run]++;
			}
			else {  //Check for overflow page incase of last run
				if(page_index[next_rec_run]==runlen)
				 {
					if(overflow[(next_rec_run+1)*runlen-1]) {
						delete page_array[next_rec_run];
						page_array[next_rec_run]=NULL;
						page_array[next_rec_run]=(overflow[(next_rec_run+1)*runlen-1]);
						overflow[(next_rec_run+1)*runlen-1]=NULL;
						page_array[next_rec_run]->GetFirst(next);
					}
					else is_rec_found=false;
				}
				else is_rec_found=false;
			}
		}
		
		//Push the next record into the priority queue if found 
		if(is_rec_found) {
			p_queue.push(next);
		}
		m_record[next]=next_rec_run;
		//rec_head[next_rec_run]=next;


		out.Insert(r); //Put the min priority record onto the pipe
		//cout<<temp_file<<endl;
	}

	f.Close(); //Tempfile close
	
	remove(temp_file); //Deleting the temp file
		
	out.ShutDown ();

}

BigQ::~BigQ() {}
