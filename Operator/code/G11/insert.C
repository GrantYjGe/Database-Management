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
/*  Insert a tuple with the given attribute values (in attrList) in relation. The value of the attribute is supplied in the attrValue member of the attrInfo structure. Since the order of the attributes in attrList[] may not be the same as in the relation, you might have to rearrange them before insertion. If no value is specified for an attribute, you should reject the insertion as Minirel does not implement NULLs.
 */
    
    // Check for NULLs in attrList
    // If any attribute is NULL, reject the insertion as Minirel does not implement NULLs
    for(int k=0; k < attrCnt; k ++)
    {
        if(attrList[k].attrValue == NULL)
        {
            return ATTRNOTFOUND;  // If NULL attr, reject the insertion.
        }
    }
    
    
    Status status;
    RelDesc rd;
    AttrDesc *attrs;
    int allAttrCnt;
    Record rec;
    
    // create buffers for variable conversion
    int intBuf;       
    float floatBuf;

    
    // get relation data
    if ((status = relCat->getInfo(relation, rd)) != OK) return status;
    
    // get attribute data
    if ((status = attrCat->getRelInfo(rd.relName, allAttrCnt, attrs)) != OK)
        return status;
    
    //calculate the total record length
    rec.length = 0;
    for(int i = 0; i < attrCnt; i ++)
    {
        rec.length += attrs[i].attrLen;
    }

    // Allocate space for the record
    char outData[rec.length];
    rec.data = &outData;

    // Create pointer to be used for the source parameter of memcpy
    void* machineReadableInputPtr;

    // Rearrange attribute list to match order of attributes in relation
    // Place the attribute data and total attribute length into a record
    int recDataOffset = 0;
    for(int i = 0; i < attrCnt; i++)
    {
        for(int j = 0; j < attrCnt; j++)
        {
            if(0 == strcmp(attrs[i].attrName,attrList[j].attrName))
            {   
		// Switch statement because all input comes in as strings
		// Integers and floats need to be converted to their respective types
		switch (attrs[i].attrType)
		{
		    case 1: //integer
			intBuf = atoi((char*)attrList[j].attrValue);
			machineReadableInputPtr = &intBuf;
			break;

		    case 2: //float
			floatBuf = atof((char*)attrList[j].attrValue);
			machineReadableInputPtr = &floatBuf;
			break;

		    case 0: //string
			machineReadableInputPtr = attrList[j].attrValue;
			break;
		}
                memcpy((char*)outData + recDataOffset, (char*)machineReadableInputPtr, attrs[i].attrLen);
                recDataOffset += attrs[i].attrLen;
            }
        }
    }
    
    // Create InsertFileScan and insert the record
    InsertFileScan ifs(relation, status);
    RID outRid;
    status = ifs.insertRecord(rec, outRid);
    if(status != OK){return status;}
    
// Return OK on success
return OK;

}
