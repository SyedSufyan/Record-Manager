#ifndef MGMT_H
#define MGMT_H

#include <stdlib.h>
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "expr.h"

//Buffer Structure used for all the implementations
typedef struct Buffer
{
	int buffer_mgr_pageNum;
	int storage_mgr_pageNum;
	BM_PageHandle *ph;
	bool dirty;//marks dirty pages
	int count;
	int freqCount;//counts the number of times the page was called 
	int fixcounts;
	struct Buffer *next;//pointer to the next buffer
} Buffer;

//Buffer Management Structure for all the Buffer to be created
typedef struct BM_BufferMgmt
{
	SM_FileHandle *f;
	Buffer *start;//marks the first buffer of the pool
	Buffer *current;
	Buffer *iterator;
	Buffer *search;
	int numReadIO;//number of Read IO done
	int numWriteIO;//number of Write IO done
} BM_BufferMgmt;

typedef struct RM_RecordMgmt
{
    BM_BufferPool *bm;
    int *freePages;
} RM_RecordMgmt;

typedef struct RM_ScanMgmt
{
    Expr *cond;
    int currentPage;
    Record *currRecord;
} RM_ScanMgmt;

#endif