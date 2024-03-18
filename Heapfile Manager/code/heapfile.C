////////////////////////////////////////////////////////////////////////////////
//                   Group Information
// Semester:         CS564 Spring 2023
// Lecturer's Name:  Xiangyao Yu
// File:             heapfile.C
// Purpose:          Implement a File Manager for Heap Files that also provides 
//					 a scan mechanism that will allow user to search a heap file
//					 for records that satisfy a search predicate called a filter.
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
#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

// Check if the file already exists
status = db.openFile(fileName, file);
if (status == OK) {
    db.closeFile(file);
    return FILEEXISTS;
}

// If the file doesn't exist, create it
status = db.createFile(fileName);
if (status != OK){
    return status;
}

// Open the newly created file
status = db.openFile(fileName, file);
if (status != OK){
    return status;
}

// Allocate and initialize the header page  
status = bufMgr->allocPage(file, hdrPageNo, newPage);
if (status != OK){
    return status;
}
hdrPage = (FileHdrPage*) newPage;

// Copy the filename to the header page
strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE); 

// Allocate and initialize the first empty data page
status = bufMgr->allocPage(file, newPageNo, newPage);
if (status != OK){
    return status;
}
newPage->init(newPageNo);

// Set up the forward pointer of the data page
status = newPage->setNextPage(-1);

// Set up header page pointers 
hdrPage->recCnt = 0;
hdrPage->pageCnt = 1;
hdrPage->firstPage = newPageNo;
hdrPage->lastPage = newPageNo;

// Unpin header page
status = bufMgr->unPinPage(file, hdrPageNo, true);
if (status != OK){
    return status;
}

// Unpin data page
status = bufMgr->unPinPage(file, newPageNo, true);
if (status != OK){
    return status;
}

// Flush the pages to disk and close the file
status = bufMgr->flushFile(file);
if (status != OK){
    return status;
}
status = db.closeFile(file);
if (status != OK){
    return status;
}
return OK;
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

	//cout << "Opening file " << fileName << endl;

	// Open the file and read in the header page and the first data page
	status = db.openFile(fileName, filePtr);
	if (status != OK)
	{
		cerr << "Error: Failed to open heap file" << endl;
		returnStatus = status;
		return;
	}

	// Read the header page into the buffer pool
	status = filePtr->getFirstPage(headerPageNo);
	if (status != OK) 
	{
		cerr << "Error: No first page number" << endl;
		returnStatus = status;
		return;
	}
	status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
	if (status != OK) 
	{
		cerr << "Error: Failed to read header page" << endl;
		returnStatus = status;
		return;
	}
	headerPage = (FileHdrPage*) pagePtr;
	hdrDirtyFlag = false;

	// Read the first data page into the buffer pool
	curPageNo = headerPage->firstPage;
	status = bufMgr->readPage(filePtr, curPageNo, curPage);
	if (status != OK) 
	{
		cerr << "Error: Failed to read data page" << endl;
		returnStatus = status;
		return;
	}
	curDirtyFlag = false;
	curRec = NULLRID; 	
	returnStatus = OK;

}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    //cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
	//cout <<  "unpinning page " << curPageNo << "with dirtyFlag " << curDirtyFlag << endl;
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
    // unpin the header page
    //cout <<  "unpinning headerPage  " << headerPageNo << "with dirtyFlag " << hdrDirtyFlag << endl;
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
    // status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
    // if (status != OK) cerr << "error in flushFile call\n";
    // before close the file
    status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID &  rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
	// If there is already a page pinned, check if it is the right page
	if (curPage != NULL)
	{
		if (rid.pageNo == curPageNo)
		{
			// The correct page is already pinned, get the record
			status = curPage->getRecord(rid, rec);
			curRec = rid;
			return status;
		}
		else
		{
			// Wrong page is pinned, unpin it before reading the right page
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) 
			{
				curPage = NULL;  
				curPageNo = 0;  
				curDirtyFlag = false;
				return status;
			}
		}
	}

	// Read the page containing the record into buffer pool
	status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
	if (status != OK){
    return status;
    }

	// Update the current page and record information
	curPageNo = rid.pageNo;
	curDirtyFlag = false;
	curRec = rid;

	// Get the record
	return curPage->getRecord(rid, rec);

}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;

    if (curPageNo < 0) return FILEEOF;  // already at EOF!

 	// Special case of the first record of the first page of the file
    if (curPage == NULL) {
        // get the first page of the file
        curPageNo = headerPage->firstPage;
        if (curPageNo == -1) {
            // File is empty
            return FILEEOF;
        }

        // Read the first page of the file
        status = bufMgr->readPage(filePtr, curPageNo, curPage); 
        curDirtyFlag = false;
        curRec = NULLRID;
        if (status != OK) {
            return status;
        } else {
            // Get the first record off the page
            status  = curPage->firstRecord(tmpRid);
            curRec = tmpRid;
            if (status == NORECORDS) {
                // First page had no records
                status = bufMgr->unPinPage(filePtr, curPageNo,curDirtyFlag);
                if (status != OK) {
                    return status;
                }

                curPageNo = -1; // In case called again
                curPage = NULL; // For endScan()
                return FILEEOF;
            }
            // Get pointer to record
            status = curPage->getRecord(tmpRid, rec);
            if (status != OK) {
                return status;
            }
            // Check if record matches predicate
            if (matchRec(rec) == true) {
                outRid = tmpRid;
                return OK;
            }
        }
    }
    // Default case. already have a page pinned in the buffer pool.
    // First see if it has any more records on it.  If so, return
    // next one. Otherwise, get the next page of the file
    for(;;) 
    {
	// Loop, looking for a record that satisfied the predicate.
	// First try and get the next record off the current page
		status = curPage->nextRecord(curRec, nextRid);
        if (status == OK) {
            curRec = nextRid;
        } else {
            while ((status == ENDOFPAGE) || (status == NORECORDS)) {
                // Get the page number of the next page in the file.
                status = curPage->getNextPage(nextPageNo);
                if (nextPageNo == -1) {
                    // End of file
                    return FILEEOF;
                }

                // Unpin the current page.
        	    status = bufMgr->unPinPage(filePtr,curPageNo, curDirtyFlag);
                curPage = NULL;
                curPageNo = -1;
                if (status != OK) {
                    return status;
                }
                
                // Get prepared to read the next page.
                curPageNo = nextPageNo;
                curDirtyFlag = false;

                // Read the next page of the file.
                status = bufMgr->readPage(filePtr, curPageNo, curPage);
                if (status != OK) {
                    return status;
                }

                // Get the first record off the page.
                status = curPage->firstRecord(curRec);
            }
        }
		
		// curRec points at a valid record
		// see if the record satisfies the scan's predicate 
		// get a pointer to the record
		status = curPage->getRecord(curRec, rec);
		if (status != OK) return status;
		// see if record matches predicate
		if (matchRec(rec) == true)  
		{
			// return rid of the record
			outRid = curRec;
			return OK;
		}
    }
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  // Heapfile constructor will read the header page and the first
  // data page of the file into the buffer pool
  // if the first data page of the file is not the last data page of the file
  // unpin the current page and read the last page
  if ((curPage != NULL) && (curPageNo != headerPage->lastPage))
  {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) cerr << "error in unpin of data page\n"; 
    	curPageNo = headerPage->lastPage;
    	status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) cerr << "error in readPage \n"; 
	curDirtyFlag = false;
  }
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
	//cout << "executing insertfilescan destructor. unpinning page " << curPageNo << endl;
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

	// Check for very large records
	if (rec.length > PAGESIZE - DPFIXED) 
	{
		// The record will never fit on a page
		return INVALIDRECLEN;
	}

	// If current page is null, make the last page the current page and read it from disk
	if (curPage == nullptr) 
	{
		curPageNo = headerPage->lastPage;
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) 
		{
			return status;
		}
	}

	// Try to add the record onto the current page. 
	status = curPage->insertRecord(rec, rid);
	if (status == OK) 
	{
		// Record insertion was successful
		headerPage->recCnt++;
		hdrDirtyFlag = true;
        curDirtyFlag = true; // Page is dirty
		outRid = rid;
		return status;
	}
	else 
	{
		// Current page was full. Allocate a new page
		status = bufMgr->allocPage(filePtr, newPageNo, newPage);
		if (status != OK) 
		{
			return status;
		}

		// Initialize the empty page
		newPage->init(newPageNo);
		status = newPage->setNextPage(-1); // No next page
		if (status != OK) 
		{
			return status;
		}

		// Modify header page contents properly
		headerPage->lastPage = newPageNo;
		headerPage->pageCnt++;
		hdrDirtyFlag = true;

		// Link up new page appropriately
		status = curPage->setNextPage(newPageNo);  // Set forward pointer
		if (status != OK) 
		{
			return status;
		}

		// Unpin current page
		status = bufMgr->unPinPage(filePtr, curPageNo, true);
		if (status != OK) 
		{
			curPage = nullptr;
			curPageNo = -1;
			curDirtyFlag = false;

			// Unpin the last page
			unpinstatus = bufMgr->unPinPage(filePtr, newPageNo, true);
			return status;
		}

		// Make the newly allocated page the current page
		curPageNo = newPageNo;
		curPage = newPage;

		// Now try to insert the record
		status = curPage->insertRecord(rec, rid);
		if (status == OK) 
		{
			curDirtyFlag = true;
			headerPage->recCnt++;
			hdrDirtyFlag = true;
			outRid = rid;
			return status;
		}
		else 
		{
			return status;
		}
	}

}