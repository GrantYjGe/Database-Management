////////////////////////////////////////////////////////////////////////////////
//                   Group Information
// Semester:         CS564 Spring 2023
// Lecturer's Name:  Xiangyao Yu
// File:             buf.C
// Purpose:          The purpose of this file is to define the BufMgr class, 
//                   which manages a buffer pool consisting of a fixed number 
//                   of frames. The file provides methods to allocate a buffer 
//                   frame, read a page into a frame, and write a page from a 
//                   frame back to disk. The BufMgr class uses a hash table to 
//                   keep track of which frames are currently holding which 
//                   pages, and it employs a clock algorithm to determine 
//                   which frame to evict when all the frames are in use.
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
#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)                                              \
    {                                                          \
        if (!(c))                                              \
        {                                                      \
            cerr << "At line " << __LINE__ << ":" << endl      \
                 << "  ";                                      \
            cerr << "This condition should hold: " #c << endl; \
            exit(1);                                           \
        }                                                      \
    }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------
/**
 * Constructor for BufMgr class that initializes the buffer manager with a given number of buffers.
 * 
 * @param bufs The number of buffers to allocate for the buffer pool.
 */
BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++)
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1;
}
/**
 * Flushes out all unwritten pages before freeing memory.
 */
BufMgr::~BufMgr()
{

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true)
        {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
}
/**
 * Find a free buffer frame and allocate it.
 *
 * @param frame Reference to an integer where the allocated buffer frame number will be stored.
 *
 * @return Status code indicating the result of the allocation operation.
 */
const Status BufMgr::allocBuf(int &frame)
{
    // find free buffer frame
    Status status = OK;
    uint32_t numScanned = 0;
    bool found = false;
    while (numScanned < 2 * numBufs)
    {
        advanceClock();
        numScanned++;

        // invalid, use it
        if (!bufTable[clockHand].valid)
        {
            break;
        }

        // valid, check refbit
        if (!bufTable[clockHand].refbit)
        {
            // check pin count
            if (bufTable[clockHand].pinCnt == 0)
            {
                // !referenced && !pinned, use it
                hashTable->remove(bufTable[clockHand].file,
                                  bufTable[clockHand].pageNo);
                found = true;
                break;
            }
        }
        else
        {
            // referenced, clear refbit
            bufStats.accesses++;
            bufTable[clockHand].refbit = false;
        }
    }

    // check if buffer is full
    if (!found && numScanned >= numBufs * 2)
    {
        return BUFFEREXCEEDED;
    }

    // flush to disk if dirty bit is set
    if (bufTable[clockHand].dirty)
    {
        bufStats.diskwrites++;

        status = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo,
                                                     &bufPool[clockHand]);
        if (status != OK)
        {
            return status;
        }
    }
    frame = clockHand;

    return OK;
}
/**
 * This function reads a page from a file into a frame in the buffer pool.
 * If the page is already in the buffer pool, it returns a pointer to the frame
 * containing the page. Otherwise, it allocates a new frame, reads the page into
 * the new frame, inserts the page into the hash table, sets up the frame
 * properly using the Set() method, and returns a pointer to the new frame
 * containing the page.
 *
 * @param file A pointer to the file from which to read the page.
 * @param PageNo The page number of the page to read.
 * @param page A pointer to the page parameter that will be set to point to
 *        the frame containing the page.
 * @return OK if the page is successfully read, otherwise an error status code.
 */
const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{
    // check whether the page is already in the buffer pool by invoking the lookup() method on the hashtable to get a frame number.
    int frameNo = 0;
    // Case 1: not in the buffer pool,allocate a new frame
    Status lookupStatus = hashTable->lookup(file, PageNo, frameNo);
    if (lookupStatus == HASHNOTFOUND)
    {

        // alloc a new frame
        Status allocStatus = allocBuf(frameNo);
        if (allocStatus != OK)
        {
            // return the status of allocBuf()
            return allocStatus;
        }

        // read the page into the new frame
        bufStats.diskreads++;
        Status readStatus = file->readPage(PageNo, &bufPool[frameNo]);
        if (readStatus != OK)
        {
            return UNIXERR;
        }

        // insert in the hash table
        Status insterStatus = hashTable->insert(file, PageNo, frameNo);
        if (insterStatus != OK)
        {
            return HASHTBLERROR;
        }

        // invoke Set() on the frame to set it up properly
        bufTable[frameNo].Set(file, PageNo);
        // return a pointer to the frame containing the page via the page parameter.
        page = &bufPool[frameNo];
        return OK;
    }
    else if (lookupStatus == OK)
    {
        // Case2 : The page is in the buffer pool.
        // set the appropriate refbit
        bufTable[frameNo].refbit = true;
        // increment the pinCnt for the page
        bufTable[frameNo].pinCnt++;
        // return a pointer to the frame containing the page via the page parameter.
        page = &bufPool[frameNo];
        return OK;
    }
    else
    {
        return HASHTBLERROR;
    }
}
/**
 * Decrements the pinCnt of the frame containing (file, PageNo) and, if
 * dirty == true, sets the dirty bit.
 *
 * @return OK if no errors occurred,
 *         HASHNOTFOUND if the page is not in the buffer pool hash table,
 *         PAGENOTPINNED if the pin count is already 0.
 */
const Status BufMgr::unPinPage(File *file, const int PageNo,
                               const bool dirty)
{
    int frameNo = 0;
    // HASHNOTFOUND if the page is in the buffer pool hash table
    if (hashTable->lookup(file, PageNo, frameNo) == OK)
    {
        if (dirty)
        { // if dirty == true sets the dirty bit
            bufTable[frameNo].dirty = dirty;
        }
        if (bufTable[frameNo].pinCnt == 0)
        { // PAGENOTPINNED if the pin count is already 0.
            return PAGENOTPINNED;
        }
        else
        { // decrement pincount
            bufTable[frameNo].pinCnt--;
        }
        return OK;
    }
    else
    { // HASHNOTFOUND if the page is not in the buffer pool hash table
        return HASHNOTFOUND;
    }
}
/**
 * Allocates a new page for a given file, and stores the page in the buffer pool
 * @param file the file in which to allocate the page
 * @param pageNo a reference to an integer which will be set to the newly allocated page number
 * @param page a reference to a Page pointer which will be set to point to the newly allocated page
 * @return a Status object indicating the success or failure of the operation
 */
const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page)
{
    int frameNo = 0;

    // allocate new page in file
    // new_page = file->allocatePage(pageNo);
    Status alocateStatus = file->allocatePage(pageNo);
    if (alocateStatus != OK)
    {
        return alocateStatus;
    }
    // get a frame from the buffer pool
    Status status = allocBuf(frameNo);
    if (status != OK)
    {
        return status; // or return BUFFEREXCEEDED, depending on the desired behavior
    }

    bufTable[frameNo].Set(file, pageNo);
    Status insterStatus = hashTable->insert(file, pageNo, frameNo);
    if (insterStatus != OK)
    {
        return insterStatus;
    }
    page = &bufPool[frameNo];

    return OK;
}
/**
 *  Disposes of a page with the given page number from the buffer pool and deallocates it in the file.
 *  @param file Pointer to the file object.
 *  @param pageNo The page number to dispose of.
 *  @return Status code indicating whether the operation was successful or not.
 */
const Status BufMgr::disposePage(File *file, const int pageNo)
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}
/**
 * This function flushes all pages belonging to the given file from the buffer pool.
 * It first checks whether each buffer in the pool is valid and belongs to the given file.
 * If a valid buffer belongs to the given file, it checks if the buffer is pinned. If it is pinned,
 * it returns PAGEPINNED status. Otherwise, if the buffer is dirty, it writes the page to disk
 * using the file's writePage function, and sets the buffer's dirty flag to false.
 * It then removes the page from the hash table, and sets the buffer's file, pageNo, and valid fields to
 * NULL, -1, and false, respectively.
 * If an invalid buffer belongs to the given file, it returns BADBUFFER status.
 *
 * @param file pointer to the file whose pages should be flushed from the buffer pool
 * @return Status code indicating success or failure of the operation
 */
const Status BufMgr::flushFile(const File *file)
{
    Status status;

    for (uint32_t i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->file == file)
        {

            if (tmpbuf->pinCnt > 0)
                return PAGEPINNED;

            if (tmpbuf->dirty == true)
            {
#ifdef DEBUGBUF
                cout << "flushing page " << tmpbuf->pageNo
                     << " from frame " << i << endl;
#endif
                if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
                                                      &(bufPool[i]))) != OK)
                    return status;

                tmpbuf->dirty = false;
            }

            hashTable->remove(file, tmpbuf->pageNo);

            tmpbuf->file = NULL;
            tmpbuf->pageNo = -1;
            tmpbuf->valid = false;
        }

        else if (tmpbuf->valid == false && tmpbuf->file == file)
            return BADBUFFER;
    }

    return OK;
}

/**
 * Print the current status of the buffer pool.
 */
void BufMgr::printSelf(void)
{
    BufDesc *tmpbuf;

    cout << endl
         << "Print buffer...\n";
    for (int i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char *)(&bufPool[i])
             << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
