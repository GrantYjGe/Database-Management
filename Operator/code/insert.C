////////////////////////////////////////////////////////////////////////////////
//                   Group Information
// Semester:         CS564 Spring 2023
// Lecturer's Name:  Xiangyao Yu
// File:             insert.C
// Purpose:          This file contains the implementation of the QU_Insert 
//                   function, which inserts a record into a specified relation. 
//                   It rearranges the attribute list to match the order of 
//                   attributes in the relation, checks for NULL attributes, 
//                   and converts input values to the appropriate data types 
//                   before inserting the record using an InsertFileScan object.
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


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
for (int i = 0; i < attrCnt; i++) {
        if (attrList[i].attrValue == NULL) {
            return ATTRNOTFOUND;
        }
    }

    Status status;
    RelDesc rd;
    // Get information about the specified relation
    if ((status = relCat->getInfo(relation, rd)) != OK) {
        return status;
    }

    int allAttrCnt;
    AttrDesc* attrs;
    // Get information about all the attributes of the relation
    if ((status = attrCat->getRelInfo(rd.relName, allAttrCnt, attrs)) != OK) {
        return status;
    }

    // Calculate the total length of the new record
    int totalLength = 0;
    for (int i = 0; i < attrCnt; i++) {
        totalLength += attrs[i].attrLen;
    }

    // Create a new Record object and char array of required length
    Record rec;
    char* recordData = new char[totalLength];
    rec.data = recordData;
    rec.length = totalLength;

    int offset = 0;
    for (int i = 0; i < attrCnt; i++) {
        // Iterate through each attribute and find corresponding attribute in attrs array
        for (int j = 0; j < allAttrCnt; j++) {
            if (strcmp(attrs[i].attrName, attrList[j].attrName) == 0) {
                // Extract value from attrList array and convert to appropriate type
                if (attrs[i].attrType == INTEGER) {
                    int value = atoi((char*)attrList[j].attrValue);
                    memcpy(recordData + offset, &value, attrs[i].attrLen);
                } else if (attrs[i].attrType == FLOAT) {
                    float value = atof((char*)attrList[j].attrValue);
                    memcpy(recordData + offset, &value, attrs[i].attrLen);
                } else {
                    memcpy(recordData + offset, attrList[j].attrValue, attrs[i].attrLen);
                }
                offset += attrs[i].attrLen;
                break;
            }
        }
    }
    // Use InsertFileScan object to insert new record into specified relation
    InsertFileScan ifs(relation, status);
    RID rid;
    status = ifs.insertRecord(rec, rid);
    
    if (status != OK) {
        return status;
    }

    return OK;
}

 