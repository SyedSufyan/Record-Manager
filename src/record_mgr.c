#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include <time.h>

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "buffer_mgr_stat.h"
#include "dberror.h"
#include "record_mgr.h"

// table and manager
extern RC initRecordManager (void *mgmtData);
extern RC shutdownRecordManager ();
extern RC createTable (char *name, Schema *schema);
extern RC openTable (RM_TableData *rel, char *name);
extern RC closeTable (RM_TableData *rel);
extern RC deleteTable (char *name);
extern int getNumTuples (RM_TableData *rel);

// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record);
extern RC deleteRecord (RM_TableData *rel, RID id);
extern RC updateRecord (RM_TableData *rel, Record *record);
extern RC getRecord (RM_TableData *rel, RID id, Record *record);

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
//need to add error check
if(rel == NULL)
return RC_NULLTABLE
if(cond == NULL)
return RC_NULLCONDITION
//Copying all the attributes from the table to the Scan Table variable

// Allocate memory for RM_tableData in scan
    scan->rel = (RM_TableData *) malloc(sizeof(RM_TableData));
    // Update table name in the scan's RM_tableData
    scan->rel->name = (char *) malloc (sizeof(char) * strlen(rel->name));
    strcpy(scan->rel->name, rel->name);

    // Update schema in scan's RM_TableData
    scan->rel->schema = createSchema(rel->schema->numAttr, rel->schema->attrNames, rel->schema->dataTypes, rel->schema->typeLength, rel->schema->keySize, rel->schema->keyAttrs)

    // Allocate memory for RM_tableMgmtData
    scan->rel->mgmtData = (RM_tableMgmtData *) malloc(sizeof(RM_tableMgmtData));
    scan->rel->mgmtData->bm = rel->mgmtData->bm;    // Bufferpool
    scan->rel->mgmtData->numRecords = rel->mgmtData->numRecords;    // Assign numRecords
    scan->rel->mgmtData->recLength = rel->mgmtData->recLength;    // Assign recLength
    scan->rel->mgmtData->totalTableBlocks = rel->mgmtData->totalTableBlocks;    // # table blocks.

    // Allocate memory for scan related management data
    scan->mgmtData = (RM_scanMgmtData *) malloc(sizeof(RM_scanMgmtData));
    scan->mgmtData->currentRID = (RID *) malloc(sizeof(RID));    // Allocate mem for RID
//    scan->mgmtData->expr = (Expr *) malloc(sizeof(Expr));       // Mem for Expression.
//    memcpy(scan->mgmtData->expr, cond, sizeof(Expr));

    scan->mgmtData->expr = cond;

    // Initialize currentRID for scan
    //scan->mgmtData->currentRID->page = 1;
    //scan->mgmtData->currentRID->slot = -1;

    return RC_OK;
}
RC next (RM_ScanHandle *scan, Record *record)
{

}
extern RC closeScan (RM_ScanHandle *scan);

// dealing with schemas
extern int getRecordSize (Schema *schema);
//simple Create Schema
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{//need to add error check ?

// Allocate memory for schema
    Schema *newSchema = (Schema *) malloc(sizeof(Schema));
    newSchema->numAttr = numAttr; // # of attributes in schema
    newSchema->keySize = keySize; // # keys in schema
    newSchema->attrNames = attrNames;
    newSchema->dataTypes = dataTypes;
    newSchema->typeLength = typeLength;
    newSchema->keyAttrs = keys;

    return newSchema;
}

extern RC freeSchema (Schema *schema);

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema);
extern RC freeRecord (Record *record);
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value);
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value);


