////////////////////////////////////////////////////////////////////////////////
//                   Group Information
// Semester:         CS564 Spring 2023
// Lecturer's Name:  Xiangyao Yu
// File:             select.C
// Purpose:          This code implements a function QU_Select that selects
//                   records from a relation based on given search criteria,
//                   and a helper function ScanSelect that performs the actual
//                   scanning and selection of records. The code retrieves the
//                   attributes' information of the requested relation and
//                   calculates the length of a record based on the projection
//                   attributes. It uses the HeapFileScan class to scan the heap
//                   file of the relation and apply the given selection criteria
//                   to filter the records. It creates a new record from the
//                   projection attributes and inserts it into the result file
//                   using the InsertFileScan class.
// Group:            11
//
// Group Member1:    Haixi Zhang
// Email:            hzhang845@wisc.edu
// CS Login:         haixi
//
// Group Member2:    Yuanjun Ge
// Email:            ge42@wisc.edu
// CS Login:         yuanjun
//
//
// Group Member3:    Xinyu Zhou
// Email:            xzhou422@wisc.edu
// CS Login:         xinyuz
////////////////////////////////////////////////////////////////////////////////

#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"

// forward declaration
const Status ScanSelect(const string &result,
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filter,
                        const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string &result,
                       const int projCnt,
                       const attrInfo projNames[],
                       const attrInfo *attr,
                       const Operator op,
                       const char *attrValue)
{
    // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

    if (attr != NULL && (attr->attrType < 0 || attr->attrType > 2))
    {
        return BADCATPARM;
    }

    Status status;
    int attrCnt;
    AttrDesc *attrDesc;
    if ((status = attrCat->getRelInfo(projNames[0].relName, attrCnt, attrDesc)) != OK)
    {
        return status;
    }

    // Create an array of attribute descriptions for the projected attributes
    AttrDesc projAttrDesc[projCnt];
    int searchAttr = -1;
    int recordLen = 0;
    // Loop through all the attributes of the relation.
    for (int i = 0; i < attrCnt; i++)
    {
        // Loop through all the projected attributes.
        for (int j = 0; j < projCnt; j++)
        {
            if (strcmp(attrDesc[i].attrName, projNames[j].attrName) == 0)
            {
                recordLen += attrDesc[i].attrLen;
                projAttrDesc[j] = attrDesc[i];
            }
        }
        // Find the attribute description for the search attribute, if given
        if (attr != nullptr && strcmp(attrDesc[i].attrName, attr->attrName) == 0)
        {
            searchAttr = i;
        }
    }
    // Set the search attribute description to NULL if no search attribute is specified.
    const AttrDesc *searchAttrDesc = NULL;
    if (searchAttr >= 0)
    {
        searchAttrDesc = &attrDesc[searchAttr];
    }
    // Call ScanSelect function to perform the actual select operation on the relation
    return ScanSelect(result, projCnt, projAttrDesc, searchAttrDesc, op, attrValue, recordLen);
}

/*
 * This function sets up a HeapFileScan on the given relation,
 * and an InsertFileScan to insert the resulting records into a new file with the given filename.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status ScanSelect(const string &result,
#include "stdio.h"
#include "stdlib.h"
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filter,
                        const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

    Status status;
    // Set up a HeapFileScan on the given relation
    HeapFileScan relFile(projNames[0].relName, status);
    if (status != OK)
    {
        return status;
    }
    // Set up an InsertFileScan to insert the resulting records into a new file with the given filename
    InsertFileScan resultFile(result, status);
    if (status != OK)
    {
        return status;
    }

    // Declare variables to store the result record data and its length
    Record resultRec;
    char *outputData = new char[reclen];
    resultRec.data = (void *)outputData;
    resultRec.length = reclen;
    int outputOffset = 0;

    // Determine the filter type and value if a filter condition is provided
    Datatype filterType;
    void *realFilter;
    if (filter != NULL)
    {
        switch (attrDesc->attrType)
        {
        case INTEGER:
            filterType = INTEGER;
            realFilter = new int(atoi(filter));
            break;
        case FLOAT:
            filterType = FLOAT;
            realFilter = new float(atof(filter));
            break;
        case STRING:
            filterType = STRING;
            realFilter = (void *)filter;
            break;
        default:
            return BADCATPARM;
        }
        // Start the scan on the relation using the given filter condition and operator
        status = relFile.startScan(attrDesc->attrOffset, attrDesc->attrLen, filterType,
                                   (char *)realFilter, op);
    }
    else
    {
        // Start the scan on the relation with no filter condition
        status = relFile.startScan(0, 0, INTEGER, nullptr, op);
    }
    if (status != OK)
    {
        return status;
    }
    RID rid;
    Record rec;
    // Loop through all records that satisfy the filter condition (or all records if no filter is specified)
    while (relFile.scanNext(rid) == OK)
    {
        if (relFile.getRecord(rec) != OK)
        {
            return status;
        }
        // Copy the selected attributes from the current record into the result record
        outputOffset = 0;
        for (int i = 0; i < projCnt; i++)
        {
            memcpy(outputData + outputOffset, (char *)rec.data + projNames[i].attrOffset,
                   projNames[i].attrLen);
            outputOffset += projNames[i].attrLen;
        }
        // Insert the result record into the result file
        RID outRID;
        status = resultFile.insertRecord(resultRec, outRID);
        if (status != OK)
        {
            return status;
        }
    }

    return OK;
}
