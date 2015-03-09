#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

typedef struct {
	Pipe *in;
	Pipe *out;
	OrderMaker* sort_order;
	int run_len;
} bigq_util;

class BigQ {

	
public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif
