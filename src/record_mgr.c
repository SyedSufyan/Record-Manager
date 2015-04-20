#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "buffer_mgr_stat.h"
#include "dberror.h"
#include "record_mgr.h"
#include "mgmt.h"

char *serSchema(Schema *schema)
{
    char *result;
    int mem = recordMemoryRequired(schema);
    int i = 10;
    char temp[10];

    result = (char *)malloc(3*mem);
    //result = schema->numAttr + " " + schema->attrNames + " " + schema->dataTypes + " " + schema->typeLength + " " + schema-> keyAttrs + " " + keySize;
    sprintf(result, "%d", schema->numAttr);
    //sprintf(temp, "%d", schema->dataTypes[0]);
    strcat(result, "\n");
    //strcat(result, temp);
    
    for(i = 0; i < schema->numAttr; i++)
    {
        strcat(result, schema->attrNames[i]);
        strcat(result, " ");
        sprintf(temp, "%d", schema->dataTypes[i]);
        strcat(result, temp);
        strcat(result, " ");
        sprintf(temp, "%d", schema->typeLength[i]);
        strcat(result, temp);
        strcat(result, "\n");
    }

    sprintf(temp, "%d", schema->keySize);
    strcat(result, temp);
    strcat(result, "\n");
    for(i = 0; i < schema->keySize; i++)
    {
        sprintf(temp, "%d", schema->keyAttrs[i]);
        strcat(result, temp);
        strcat(result, " ");
    }
    strcat(result, "\n");
    
    return result;
}

Schema *deserSchema(char *name)
{
    int a, i, j;
    char temp[10];
    char *resultTemp;
    resultTemp = (char *)malloc(10);

    SM_FileHandle fh;
    SM_PageHandle ph;
    ph = (SM_PageHandle) malloc(PAGE_SIZE);

    a = openPageFile(name, &fh);

    if(a == RC_OK)
    {
        a = readFirstBlock(&fh, ph);

        if(a == RC_OK)
        {
            sprintf(resultTemp, "%c", ph[0]);

            for(i = 1; i < strlen(ph); i++)
            {
                sprintf(temp, "%c", ph[i]);

                if(strcmp(temp,"\n") == 0)
                    break;
                else
                    strcat(resultTemp, temp);
            }

            int numAttr = atoi(resultTemp);
            char **attrNames = (char **) malloc(sizeof(char*) * numAttr);
            DataType *dataTypes = (DataType *) malloc(sizeof(DataType) * numAttr);
            int *typeLength = (int *) malloc(sizeof(int) * numAttr);;

            for(j = 0; j < numAttr; j++)
            {
                free(resultTemp);
                resultTemp = (char *)malloc(10);
                int k = 1;

                for(i = i + 1; i < strlen(ph) ; i++)
                {
                    sprintf(temp, "%c", ph[i]);

                    if(strcmp(temp," ") == 0)
                    {
                        if(k == 1)
                        {
                            attrNames[j] = (char *) malloc(2);
                            strcpy(attrNames[j], resultTemp);
                        }
                        else if(k == 2)
                        {
                            dataTypes[j] = atoi(resultTemp);
                        }

                        k++;
                        free(resultTemp);
                        resultTemp = (char *)malloc(10);
                    }
                    else if(strcmp(temp, "\n") == 0)
                    {
                        typeLength[j] = atoi(resultTemp);
                        break;
                    }
                    else
                    {
                        sprintf(temp, "%c", ph[i]);
                        strcat(resultTemp, temp);
                    }
                }
            }
            /*
            for(j = 0 ; j < numAttr; j++)
            {
                printf("attrNames[%d]: %s\n", j, attrNames[j]);
                printf("dataTypes[%d]: %d\n", j, dataTypes[j]);
                printf("typeLength[%d]: %d\n", j, typeLength[j]);
            }
            */
            free(resultTemp);
            resultTemp = (char *)malloc(10);

            for(i = i + 1; i < strlen(ph); i++)
            {
                sprintf(temp, "%c", ph[i]);

                if(strcmp(temp,"\n") == 0)
                    break;
                else
                    strcat(resultTemp, temp);
            }

            int keySize = atoi(resultTemp);
            int *keyAttrs = (int *) malloc(sizeof(int) *keySize);

            for(j = 0; j < keySize; j++)
            {
                free(resultTemp);
                resultTemp = (char *)malloc(10);

                for(i = i + 1; i < strlen(ph); i++)
                {
                    sprintf(temp, "%c", ph[i]);

                    if(strcmp(temp," ") == 0)
                    {
                        keyAttrs[j] = atoi(resultTemp);
                    }
                    if(strcmp(temp,"\n") == 0)
                        break;
                    else
                        strcat(resultTemp, temp);
                }
            }

            //printf("keyAttrs : %d\n", keyAttrs[0]);

            Schema *newSchema = (Schema *) malloc(sizeof(Schema));
            newSchema->numAttr = numAttr;
            newSchema->attrNames = attrNames;
            newSchema->dataTypes = dataTypes;
            newSchema->typeLength = typeLength;
            newSchema->keyAttrs = keyAttrs;
            newSchema->keySize = keySize;

            return newSchema;
        }
    }
}

int recordMemoryRequired(Schema *schema)
{
    int i, memoryRequired = 0;

    for(i = 0; i < schema->numAttr; i++)
    {
        if(schema->dataTypes[i] == DT_INT)
            memoryRequired += sizeof(int);
        else if(schema->dataTypes[i] == DT_FLOAT)
            memoryRequired += sizeof(float);
        else if(schema->dataTypes[i] == DT_BOOL)
            memoryRequired += sizeof(bool);
        else
            memoryRequired += schema->typeLength[i];
    }

    return memoryRequired;
}

// table and manager
extern RC initRecordManager (void *mgmtData)
{
    return RC_OK;
}

extern RC shutdownRecordManager ()
{
    return RC_OK;
}

extern RC createTable (char *name, Schema *schema)
{
    int a;
    char *schemaData;

    a = createPageFile(name);
    if(a == RC_OK)
    {
        SM_FileHandle fh;

        a = openPageFile(name, &fh);
        if(a == RC_OK)
        {
            schemaData = serSchema(schema);

            a = writeBlock(0, &fh, schemaData);
            if(a == RC_OK)
            {
                free(schemaData);
                return RC_OK;
            }
        }        
    }
    return a;
}

extern RC openTable (RM_TableData *rel, char *name)
{
    int a;
    
    RM_RecordMgmt *mgmt;

    mgmt = (RM_RecordMgmt *)malloc(sizeof(RM_RecordMgmt));
    mgmt->bm = MAKE_POOL();

    mgmt->bm->mgmtData = NULL;

    a = initBufferPool(mgmt->bm, name, 4, RS_FIFO, NULL);
    
    if(a == RC_OK)
    {
        mgmt->freePages = (int *) malloc(sizeof(int));
        //mgmt->freePages[0] = mgmt->fh->totalNumPages;
        mgmt->freePages[0] = ((BM_BufferMgmt *)(mgmt->bm)->mgmtData)->f->totalNumPages;

        rel->name = name;
        rel->mgmtData = mgmt;
        rel->schema = deserSchema(name);

        return RC_OK;
    }

    return a;
}

extern RC closeTable (RM_TableData *rel)
{
    int a;

    a = shutdownBufferPool(((RM_RecordMgmt *)rel->mgmtData)->bm);
    if(a == RC_OK)
    {
        ((RM_RecordMgmt *)rel->mgmtData)->bm->mgmtData = NULL;
        ((RM_RecordMgmt *)rel->mgmtData)->bm = NULL;

        free(((RM_RecordMgmt *)rel->mgmtData)->freePages);
        ((RM_RecordMgmt *)rel->mgmtData)->freePages = NULL;

        //free(((RM_RecordMgmt *)rel->mgmtData)->fh);
        //((RM_RecordMgmt *)rel->mgmtData)->fh = NULL;

        free(rel->mgmtData);
        rel->mgmtData = NULL;

        free(rel->schema);
        rel->schema = NULL;

        return RC_OK;
    }

    return a;
}

extern RC deleteTable (char *name)
{
    int a;

    a = destroyPageFile(name);
    
    if(a == RC_OK)
        return RC_OK;

    return a;
}

extern int getNumTuples (RM_TableData *rel)
{

}

// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record)
{
    int a;
    //printf("record: %s\n", record->data);
    //printf("free page: %d\n", ((RM_RecordMgmt *)rel->mgmtData)->freePages[0]);

    BM_PageHandle *page = MAKE_PAGE_HANDLE();

    a = pinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page, ((RM_RecordMgmt *)rel->mgmtData)->freePages[0]);
    
    if(a == RC_OK)
    {
        //printf("currPos : %i \n", ((RM_RecordMgmt *)rel->mgmtData)->fh->curPagePos);
        
        sprintf(page->data, "%s", record->data);
        //printf("data: %s\n", page->data);

        //printf("Total Page Number: %i \n", ((RM_RecordMgmt *)rel->mgmtData)->fh->totalNumPages);
        //a = writeBlock(((RM_RecordMgmt *)rel->mgmtData)->freePages[0], ((BM_BufferMgmt *)(((RM_RecordMgmt *)rel->mgmtData)->bm)->mgmtData)->f, page->data);
        //printf("RC: %d write block\n", a);

        a = markDirty(((RM_RecordMgmt *)rel->mgmtData)->bm, page);
        if(a == RC_OK)
        {
            a = unpinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);
            if(a == RC_OK)
            {
                a = forcePage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);
                if(a == RC_OK)
                {
                    record->id.page = ((RM_RecordMgmt *)rel->mgmtData)->freePages[0];
                    record->id.slot = 0;

                    printf("record data: %s\n", record->data);
                    //printf("length of record: %i\n", strlen(record->data));
                    //printf("size of record: %i\n", sizeof(record->data));

                    free(page);

                    ((RM_RecordMgmt *)rel->mgmtData)->freePages[0] += 1;

                    return RC_OK;
                }
            }
        }
    }

    return a;
}

extern RC deleteRecord (RM_TableData *rel, RID id)
{
    int a, i;

    //printf("Record id.page: %i\n", id.page);
    //printf("Record id.slot: %i\n", id.slot);

    if(id.page > 0 && id.page <=  ((BM_BufferMgmt *)(((RM_RecordMgmt *)rel->mgmtData)->bm)->mgmtData)->f->totalNumPages)
    {
        BM_PageHandle *page = MAKE_PAGE_HANDLE();
        a = pinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page, id.page);

        if(a == RC_OK)
        {
            //printf("page->data: %s\n", page->data);
            memset(page->data, '\0', strlen(page->data));
            //sprintf(page->data, "%s", '\0');

            a = markDirty(((RM_RecordMgmt *)rel->mgmtData)->bm, page);
            if(a == RC_OK)
            {
                a = unpinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);
                if(a == RC_OK)
                {
                    a = forcePage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);
                    if(a == RC_OK)
                    {
                        free(page);
                        return RC_OK;
                    }
                }
            }
        }
        return a;
    }
    return RC_RM_RECORD_NOT_FOUND_ERROR;
}

extern RC updateRecord (RM_TableData *rel, Record *record)
{
    int a;
    
    //printf("record: %s\n", record->data);
    //printf("record id.page: %i\n", record->id.page);
    //printf("record id.slot: %i\n", record->id.slot);
    //printf("totalNumPages : %i\n", ((BM_BufferMgmt *)(((RM_RecordMgmt *)rel->mgmtData)->bm)->mgmtData)->f->totalNumPages);
    
    if(record->id.page > 0 && record->id.page <=  ((BM_BufferMgmt *)(((RM_RecordMgmt *)rel->mgmtData)->bm)->mgmtData)->f->totalNumPages)
    {
        BM_PageHandle *page = MAKE_PAGE_HANDLE();
        a = pinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page, record->id.page);

        if(a == RC_OK)
        {
            sprintf(page->data, "%s", record->data);

            a = markDirty(((RM_RecordMgmt *)rel->mgmtData)->bm, page);
            if(a == RC_OK)
            {
                a = unpinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);
                if(a == RC_OK)
                {
                    a = forcePage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);
                    if(a == RC_OK)
                    {
                        free(page);
                        return RC_OK;
                    }
                }
            }
        }
        return a;
    }
    return RC_RM_RECORD_NOT_FOUND_ERROR;
}

extern RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    int a;

    //SM_PageHandle ph;
    //ph = (SM_PageHandle) malloc(PAGE_SIZE);

    //a = readBlock(id.page, ((BM_BufferMgmt *)(((RM_RecordMgmt *)rel->mgmtData)->bm)->mgmtData)->f, ph);
    
    if(id.page > 0 && id.page <=  ((BM_BufferMgmt *)(((RM_RecordMgmt *)rel->mgmtData)->bm)->mgmtData)->f->totalNumPages)
    {
        BM_PageHandle *page = MAKE_PAGE_HANDLE();
        a = pinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page, id.page);

        if(a == RC_OK)
        {
            record->id = id;
            record->data = page->data;

            a = unpinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);
            if(a == RC_OK)
            {
                free(page);
                return RC_OK;
            }
        }
        return a;
    }
    return RC_RM_RECORD_NOT_FOUND_ERROR;
}

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    //printf("Expr Consdition, Type: %i\n", cond->type);
    //printf("Expr Consdition, Value->DataType: %i\n", cond->expr.cons->dt);
    //printf("Expr Consdition, Value->Value: %i\n", cond->expr.cons->v.intV);
    //printf("Expr Consdition, Value->Value: %s\n", cond->expr.cons->v.stringV);
    //printf("Expr Consdition, Value->Value: %f\n", cond->expr.cons->v.floatV);
    //printf("Expr Consdition, Value->Value: %d\n", cond->expr.cons->v.boolV);
    //printf("Expr Consdition, attrRef: %d\n", cond->expr.attrRef);
    //printf("Expr Consdition, Operator Type: %d\n", cond->expr.op->type);

    if (rel == NULL)
        return RC_RM_TABLE_DATA_NOT_INIT;

    RM_ScanMgmt *mgmt;
    mgmt = (RM_ScanMgmt *)malloc(sizeof(RM_ScanMgmt));
    mgmt->currRecord = (Record *)malloc(sizeof(Record));

    mgmt->cond = cond;
    mgmt->currentPage = 1;
    mgmt->flag = 0;

    scan->rel = rel;
    scan->mgmtData = mgmt;

    return RC_OK;
}

RC next (RM_ScanHandle *scan, Record *record)
{
    int a;
    Value *result;
    RID rid;
    //Record *currRecord;
    //RM_TableData *_rel = scan->rel;

    rid.page = ((RM_ScanMgmt *)scan->mgmtData)->currentPage;
    rid.slot = 0;
    //printf("((RM_ScanMgmt *)scan->mgmtData)->currentPage: %i\n", ((RM_ScanMgmt *)scan->mgmtData)->currentPage);
    //printf("((BM_BufferMgmt *)(((RM_RecordMgmt *)(scan->rel)->mgmtData)->bm)->mgmtData)->f->totalNumPages : %i\n", ((BM_BufferMgmt *)(((RM_RecordMgmt *)(scan->rel)->mgmtData)->bm)->mgmtData)->f->totalNumPages);

    if(rid.page > 0 && rid.page < ((BM_BufferMgmt *)(((RM_RecordMgmt *)(scan->rel)->mgmtData)->bm)->mgmtData)->f->totalNumPages)
    {
        //currRecord = (Record *)malloc(sizeof(Record));

        //printf("record: %s\n", record->data);
        //printf("record id.page: %i\n", record->id.page);
        //printf("record id.slot: %i\n", record->id.slot);

        //printf("cond->type: %i\n", ((RM_ScanMgmt *)scan->mgmtData)->cond->type);
        
        a = getRecord (scan->rel, rid, ((RM_ScanMgmt *)scan->mgmtData)->currRecord);

        //printf("record: %s\n", ((RM_ScanMgmt *)scan->mgmtData)->currRecord->data);
        //printf("record id.page: %i\n", ((RM_ScanMgmt *)scan->mgmtData)->currRecord->id.page);
        //printf("record id.slot: %i\n", ((RM_ScanMgmt *)scan->mgmtData)->currRecord->id.slot);

        //printf("cond->type: %i\n", type);

        //((RM_ScanMgmt *)scan->mgmtData)->cond->type = type;

        //printf("cond->type: %i\n", ((RM_ScanMgmt *)scan->mgmtData)->cond->type);
        //printf("Expr Consdition, Operator Type: %d\n", ((RM_ScanMgmt *)scan->mgmtData)->cond->expr.op->type);

        a = evalExpr (((RM_ScanMgmt *)scan->mgmtData)->currRecord, scan->rel->schema, ((RM_ScanMgmt *)scan->mgmtData)->cond, &result);

        //printf("RC: %d Next scan\n", a);
        //printf("Result: %i \n", result->v.boolV);

        if(result->v.boolV)
        {
            record->data = ((RM_ScanMgmt *)scan->mgmtData)->currRecord->data;
            record->id = ((RM_ScanMgmt *)scan->mgmtData)->currRecord->id;
            ((RM_ScanMgmt *)scan->mgmtData)->flag = 1;
            ((RM_ScanMgmt *)scan->mgmtData)->currentPage = ((RM_ScanMgmt *)scan->mgmtData)->currentPage + 1;
            return;
        }
        else
        {
            //*flag = 0;
            //printf("Recursive Function Impl\n");
            
            //free(currRecord);
            //currRecord = NULL;

            ((RM_ScanMgmt *)scan->mgmtData)->flag = 0;
            ((RM_ScanMgmt *)scan->mgmtData)->currentPage = ((RM_ScanMgmt *)scan->mgmtData)->currentPage + 1;
            next(scan, record);
            //printf("Getting back......\n");
        }
    }
    //printf("Flag : %i\n", ((RM_ScanMgmt *)scan->mgmtData)->flag);
    //printf("Current record: %s\n", ((RM_ScanMgmt *)scan->mgmtData)->currRecord->data);
    if(((RM_ScanMgmt *)scan->mgmtData)->flag == 1)
    {
        //record->data = ((RM_ScanMgmt *)scan->mgmtData)->currRecord->data;
        //record->id = ((RM_ScanMgmt *)scan->mgmtData)->currRecord->id;
        //printf("record->data: %s\n", record->data);
        return RC_OK;
    }

    ((RM_ScanMgmt *)scan->mgmtData)->flag = 0;
    ((RM_ScanMgmt *)scan->mgmtData)->currentPage = 1;
    
    return RC_RM_NO_MORE_TUPLES;

}

extern RC closeScan (RM_ScanHandle *scan)
{
    free(((RM_ScanMgmt *)scan->mgmtData)->currRecord);
    ((RM_ScanMgmt *)scan->mgmtData)->currRecord = NULL;

    free(scan->mgmtData);
    scan->mgmtData = NULL;

    free(scan);
    scan = NULL;

    return RC_OK;
}

// dealing with schemas
extern int getRecordSize (Schema *schema)
{
    int memoryRequired = recordMemoryRequired(schema);
    //printf("getRecordSize memoryRequired : %i\n", (memoryRequired + schema->numAttr + 1));
    return((memoryRequired)/2);
    //return 10;
}

//simple Create Schema
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    Schema *newSchema = (Schema *) malloc(sizeof(Schema));
    newSchema->numAttr = numAttr;
    newSchema->attrNames = attrNames;
    newSchema->dataTypes = dataTypes;
    newSchema->typeLength = typeLength;
    newSchema->keyAttrs = keys;
    newSchema->keySize = keySize;

    return newSchema;
}

extern RC freeSchema (Schema *schema)
{
    schema->numAttr = NULL;
    schema->attrNames = NULL;
    schema->dataTypes = NULL;
    schema->typeLength = NULL;
    schema->keyAttrs = NULL;
    schema->keySize = NULL;

    free(schema);
    schema = NULL;

    return RC_OK;
}

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema)
{
    int i;
    int memoryRequired = recordMemoryRequired(schema);
    //printf("memoryRequired : %i\n", (memoryRequired + schema->numAttr + 1));
    *record = (Record *)malloc(sizeof(Record));
    record[0]->data = (char *)malloc(memoryRequired + schema->numAttr + 1);
    
    sprintf(record[0]->data, "%s", "(");
    for(i = 0; i < schema->numAttr - 1; i++)
    {
        strcat(record[0]->data,",");
    }
    strcat(record[0]->data,")");
    
    return RC_OK;
}

extern RC freeRecord (Record *record)
{
    free(record->data);
    record->data = NULL;

    free(record);
    record = NULL;

    return RC_OK;
}

extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    int offset = 0, i;
    char temp[1000];
    char *pre, *result;

    int mem = recordMemoryRequired(schema);
    //result = (char *)malloc(schema->typeLength[i]);

    //printf("record : %s\n", record->data);
    //printf("attrNum : %i\n", attrNum);

    if(attrNum < schema->numAttr)
    {
        pre = (char *)malloc(mem);
        result = (char *)malloc(schema->typeLength[attrNum]);

        if(attrNum == 0)
        {
            sprintf(pre, "%s", "(");
            sprintf(result, "%c", record->data[1]);

            for(i = 2; i < strlen(record->data); i++)
            {
                if(record->data[i] == ',')
                    break;

                sprintf(temp, "%c", record->data[i]);
                strcat(result, temp);
            }
        }
        else if(attrNum > 0 && attrNum < schema->numAttr)
        {
            int reqNumCommas = attrNum, numCommas = 0;

            sprintf(pre, "%s", "(");

            for(i = 1; i < strlen(record->data); i++)
            {
                if(numCommas == reqNumCommas)
                {
                    if(record->data[i] == ',' || record->data[i] == ')')
                        break;

                    sprintf(temp, "%c", record->data[i]);
                    strcat(result, temp);
                    continue;
                }

                if(record->data[i] == ',')
                {
                    sprintf(result, "%c", record->data[++i]);
                    numCommas++;
                }

                sprintf(temp, "%c", record->data[i]);
                strcat(pre, temp);
            }
        }

        //printf("result: %s\n", result);

        Value *val = (Value*) malloc(sizeof(Value));
        if(schema->dataTypes[attrNum] == DT_INT)
            val->v.intV = atoi(result);
        else if(schema->dataTypes[attrNum] == DT_FLOAT)
            val->v.floatV = atof(result);
        else if(schema->dataTypes[attrNum] == DT_BOOL)
            val->v.boolV = (bool) *result;
        else
            val->v.stringV = result;

        val->dt = schema->dataTypes[attrNum];
        value[0] = val;
        //printf("result: %s\n", value[0]->v.stringV);
        //printf("serserializeValue : %s\n", serializeValue(value[0]));

        //free(pre);
        //free(result);
        //pre = NULL;
        //result = NULL;

        return RC_OK;
    }

    return RC_RM_NO_MORE_TUPLES;
}

extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    int offset = 0, i;
    char temp[1000];
    char *pre, *post;

    int mem = recordMemoryRequired(schema);
    pre = (char *)malloc(mem);
    post = (char *)malloc(mem);
    //sprintf(record->data, "%s", "(");
    //printf("record : %s\n", record->data);
    //printf("%d \n", attrNum);
    //printf("%d \n", strlen(record->data));
    //printf("%c \n", record->data[1]);
    //printf("Value Int: %i\n", value->v.intV);
    //printf("Value String: %s\n", value->v.stringV);
    //printf("Value Float: %i\n", value->v.floatV);
    //printf("Value Boolean: %i\n", value->v.boolV);
    if(attrNum < schema->numAttr)
    {
        if(attrNum == 0)
        {
            sprintf(pre, "%s", "(");

            for(i = 1; i < strlen(record->data); i++)
            {
                if(record->data[i] == ',')
                    break;

                sprintf(temp, "%c", record->data[i]);
                strcat(pre, temp);
            }
            
            sprintf(post, "%s", ",");

            for( i = i + 1; i < strlen(record->data); i++)
            {
                sprintf(temp, "%c", record->data[i]);
                strcat(post, temp);
            }

            //printf("Pre: %s\n", pre);
            //printf("Post: %s\n", post);
        }
        else if(attrNum > 0 && attrNum < schema->numAttr)
        {
            int reqNumCommas = attrNum, numCommas = 0;

            sprintf(pre, "%s", "(");

            for(i = 1; i < strlen(record->data); i++)
            {
                if(numCommas > reqNumCommas)
                    break;

                if(numCommas == reqNumCommas)
                {
                    if(record->data[i] == ',')
                        numCommas++;
                    continue;
                }

                if(record->data[i] == ',')
                    numCommas++;

                sprintf(temp, "%c", record->data[i]);
                strcat(pre, temp);
            }

            if(attrNum != (schema->numAttr - 1))
            {
                sprintf(post, "%s", ",");

                for( ; i < strlen(record->data); i++)
                {
                    sprintf(temp, "%c", record->data[i]);
                    strcat(post, temp);
                }
            }
            else
                sprintf(post, "%s", ")");

            //printf("Pre: %s\n", pre);
            //printf("Post: %s\n", post);
        }

        if(schema->dataTypes[attrNum] == DT_INT)
            sprintf(temp, "%d", value->v.intV);
        else if(schema->dataTypes[attrNum] == DT_FLOAT)
            sprintf(temp, "%f", value->v.floatV);
        else if(schema->dataTypes[attrNum] == DT_BOOL)
            sprintf(temp, "%d", value->v.boolV);
        else
            strcpy(temp, value->v.stringV);

        strcpy(record->data, pre);
        strcat(record->data, temp);
        strcat(record->data, post);

        //printf("record : %s\n", record->data);
        free(pre);
        free(post);
        pre = NULL;
        post = NULL;

        return RC_OK;
    }

    return RC_RM_NO_MORE_TUPLES;
}