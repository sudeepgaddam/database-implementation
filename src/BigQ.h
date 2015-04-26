#ifndef BIGQ_H
#define BIGQ_H
#include <algorithm>
#include <iostream>
#include <queue>
#include "Pipe.h"
#include <math.h>
#include "File.h"
#include "Record.h"
#include "ComparisonEngine.h"
#include <vector>
#include <map>

using namespace std;
class ComparisonEngine;

typedef struct {
	Pipe *in;
	Pipe *out;
	OrderMaker* sort_order;
	int run_len;
} bigq_util;


class BigQ  {
public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};



#endif



