////////////////////////////////////////////////////////////////////////////////
//                   Group Information
// Semester:         CS564 Spring 2023
// Lecturer's Name:  Xiangyao Yu
// File:             insert.C
// Purpose:          This file is part of a database management system and 
//                   implements a function called QU_Delete, which deletes 
//                   records from a specified table based on a given search 
//                   condition. It uses helper functions and classes defined 
//                   in other modules and is designed to provide a way to delete 
//                   database records that meet certain criteria.
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
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 *
 * Log:
 * 2012/11/26 JH: First implementation.
 * 2012/12/03 JH: Condition value conversion added. Can delete all tuples.
 */
const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
    // rejects when no relation or attribute is given
    if (relation.empty()) return BADCATPARM;
    
    Status status;
    RID rid;
    HeapFileScan *hfs = new HeapFileScan(relation, status);
    if (status!=OK) return status;
    
    AttrDesc aDesc;
    void* filter;
    int iVal;
    float fVal;
    
    // If an attribute name is provided, use it to set up the scan.
    if (attrName!="") {
        // a search condition is specified;
        // find attribute info to set up scan
        
        switch (type) {
            case STRING: filter = (void*)attrValue; break;
            case INTEGER: {
                iVal = atoi(attrValue);
                filter = &iVal;
                break;
            }
            case FLOAT: {
                fVal = atof(attrValue);
                filter = &fVal;
                break;
            }
        }
        status = attrCat->getInfo(relation, attrName, aDesc);
        if (status!=OK) return status;
        hfs->startScan(aDesc.attrOffset, aDesc.attrLen, type, (char*)filter, op);
    } else {
        // If no attribute name is provided, delete all tuples in the relation.
        hfs->startScan(0, 0, type, NULL, op);
    }
    
    // if error occurs during startScan, the loop below is skipped and exit steps are taken
    
    // Scan the relation and delete matching records.
    while (status==OK) {
        status = hfs->scanNext(rid);
        if (status == FILEEOF) break;
        
        hfs->deleteRecord();
    }
    
    // delete record
    delete hfs;
    return OK;
}