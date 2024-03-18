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
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
    //check relation is not empty
    if (relation.empty() || !attrName.empty() && !attrValue){
        return BADCATPARM;
     }
    
   Status status;
   // Create a heap file scan object for the given relation
    HeapFileScan* hfs = new HeapFileScan(relation, status);
    if (status != OK) {
        //delete hfs;
        return status;
    }
    
    // If an attribute name is provided, get attribute information from attribute catalog for the given relation
    if (!attrName.empty()) {
        AttrDesc aDesc;
        status = attrCat->getInfo(relation, attrName, aDesc);
        if (status != OK) {
            delete hfs;
            return status;
        }
        // Create a filter object based on the given data type and operator
        void* filter = NULL;
        switch (type) {
            case INTEGER: {
                int iVal = atoi(attrValue);
                filter = &iVal;
                break;
            }
            case FLOAT: {
                float fVal = atof(attrValue);
                filter = &fVal;
                break;
            }
            case STRING: {
                filter = (void*)attrValue;
                break;
            }
            default:
                // Unsupported data type
                delete hfs;
                return BADCATPARM;
        }
        // Start the heap file scan with the filter object and given operator
        hfs->startScan(aDesc.attrOffset, aDesc.attrLen, type, (char*)filter, op);
    } else {
        // No search condition, so start the scan without a filter
        hfs->startScan(0, 0, type, nullptr, op);
    }

    // Iterate through the matching records and delete them
    RID rid;
    while ((status = hfs->scanNext(rid)) == OK) {
        status = hfs->deleteRecord();
        if (status != OK) {
            delete hfs;
            return status;
        }
    }
    if (status != FILEEOF) {
        delete hfs;
        return status;
    }

    // Clean up and return success
    delete hfs;
    return OK;
}



