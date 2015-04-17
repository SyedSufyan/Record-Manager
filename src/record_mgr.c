#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "buffer_mgr_stat.h"
#include "dberror.h"
#include "record_mgr.h"


typedef struct RM_RecordMgmt
{
    BM_BufferPool *bm;
    SM_FileHandle *fh;
    int *freePages;

} RM_RecordMgmt;

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
    mgmt->fh = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));

    int ret = openPageFile(name, mgmt->fh);

    if(ret == RC_FILE_NOT_FOUND)
        return RC_FILE_NOT_FOUND;

    mgmt->freePages = (int *) malloc(sizeof(int));
    mgmt->freePages[0] = mgmt->fh->totalNumPages;

    a = initBufferPool(mgmt->bm, name, 4, RS_FIFO, NULL);
    
    if(a == RC_OK)
    {
        rel->name = name;
        rel->mgmtData = mgmt;
        rel->schema = deserSchema(name);

        return RC_OK;
    }

    return a;
}

extern RC closeTable (RM_TableData *rel)
{

}

extern RC deleteTable (char *name)
{

}

extern int getNumTuples (RM_TableData *rel)
{

}

// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record)
{
    int page_no = 1, a;
    printf("record: %s\n", record->data);
    printf("free page: %d\n", ((RM_RecordMgmt *)rel->mgmtData)->freePages[0]);

    BM_PageHandle *page=MAKE_PAGE_HANDLE();

    a = pinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page, ((RM_RecordMgmt *)rel->mgmtData)->freePages[0]);

    sprintf(page->data, "%s", record->data);

    markDirty(((RM_RecordMgmt *)rel->mgmtData)->bm, page);
    unpinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);

    record->id.page = ((RM_RecordMgmt *)rel->mgmtData)->freePages[0];
    record->id.slot = 0;

    free(page);

    ((RM_RecordMgmt *)rel->mgmtData)->freePages[0] += 1;

    return RC_OK;
}

extern RC deleteRecord (RM_TableData *rel, RID id)
{

}

extern RC updateRecord (RM_TableData *rel, Record *record)
{

}

extern RC getRecord (RM_TableData *rel, RID id, Record *record)
{

}

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    
}
RC next (RM_ScanHandle *scan, Record *record)
{

}
extern RC closeScan (RM_ScanHandle *scan)
{

}

// dealing with schemas
extern int getRecordSize (Schema *schema)
{

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

}

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema)
{
    int i;
    int memoryRequired = recordMemoryRequired(schema);

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

}

extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{

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
            sprintf(temp, "%f", value->v.boolV);
        else
            strcpy(temp, value->v.stringV);

        strcpy(record->data, pre);
        strcat(record->data, temp);
        strcat(record->data, post);

        //printf("record : %s\n", record->data);

        return RC_OK;
    }

    return RC_RM_NO_MORE_TUPLES;
}