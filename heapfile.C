#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* file;
    Status status;
    FileHdrPage* hdrPage;
    int hdrPageNo;
    int newPageNo;
    Page* newPage;

    // Try to open the file
    status = db.openFile(fileName, file);
    if (status != OK)
    {
        // Create the file if it does not exist
        status = db.createFile(fileName);
        if (status != OK) return status;

        // Open the newly created file
        status = db.openFile(fileName, file);
        if (status != OK) return status;

        // Allocate the header page
        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (status != OK) return status;

        // Initialize header page values
        hdrPage = (FileHdrPage*)newPage;
        strcpy(hdrPage->fileName, fileName.c_str());
        // hdrPage->firstPage = -1; // Initialize to -1 indicating no data pages yet
        // hdrPage->lastPage = -1;
        // hdrPage->pageCnt = 0;
        // hdrPage->recCnt = 0;

        // Allocate the first data page
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK) return status;

        // Initialize the first data page
        newPage->init(newPageNo);

        // Update header page information
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt = 1;
        hdrPage->recCnt = 0;

        // Unpin the header page and mark it as dirty
        status = bufMgr->unPinPage(file, hdrPageNo, true);
        if (status != OK) return status;

        // Unpin the first data page and mark it as dirty
        status = bufMgr->unPinPage(file, newPageNo, true);
        if (status != OK) return status;

        // Close the file
        status = bufMgr->flushFile(file);
        if (status != OK) return status;
        status = db.closeFile(file);
        if (status != OK) return status;

        return OK;
    }
    return FILEEXISTS;
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

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {   
        // get header page number
        status = filePtr->getFirstPage(headerPageNo);
        if(status != OK) {
            cerr << "open of heap file failed\n";
            returnStatus = status;
            return;
        } 

        // allocating the page into the buffer
        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
        if(status != OK) {
            cerr << "open of heap file failed\n";
            returnStatus = status;
            return; 
        }

        // cout << "Page count: " + std::to_string(headerPageNo) + "\n";

        
        // initializing private data members
        headerPage = (FileHdrPage*) pagePtr;
        hdrDirtyFlag = true; 
        curPageNo = headerPage->firstPage;
        // cout << "Page count: " + std::to_string(headerPage->pageCnt) + "\n";
        // cout << "Record count: " + std::to_string(headerPage->recCnt) + "\n";

        // reading the first page of file into buffer pool
        status = bufMgr->readPage(filePtr, curPageNo, pagePtr);
        if(status != OK) {
            cerr << "open of heap file failed\n";
            returnStatus = status;
            return; 
        }
        
        curPage = pagePtr;
        curDirtyFlag = false;
        curRec = NULLRID;
        returnStatus = OK;
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
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

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status statusOne;
    Status statusTwo;
    Status statusThree;


    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
   if (rid.pageNo != curPageNo) { // // check if the requested record's page number differs from the current page number
        statusOne = bufMgr-> unPinPage(filePtr, curPageNo, curDirtyFlag); // If true, unpin the current page to release its buffer slot

        //cout << "I am here\n";
        if (statusOne != OK){
            return statusOne;
        }
        statusTwo = bufMgr->readPage(filePtr, rid.pageNo, curPage); // Read the page containing the requested record into memory
        if (statusTwo!= OK) {
            return statusTwo;
        }
        // Update the current page number and reset the dirty flag
        curPageNo = rid.pageNo; 
        curDirtyFlag = false;
   }
    // Get the record corresponding to the given RID
    // cout << "I am actually here!\n";
    // cout << "Current page number = " + std::to_string(curPageNo) + "\n";
    // cout << "Record ID pageNo = " + std::to_string(rid.pageNo) + "\n";
    // cout << "Record ID slotNo = " + std::to_string(rid.slotNo) + "\n";
    // RID dummyRID;
    // cout << "current page details = " + std::to_string((curPage->firstRecord(dummyRID)));
   statusThree = curPage-> getRecord(rid, rec);
   
   if (statusThree != OK) {
    return statusThree;
   }
   // update RID
   curRec = rid;
   return OK;
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
    Record  rec;
    bool    recLoop  = false;
    bool    pageLoop = false;
   
    // if curPage is NULL, start from page 1
    if (curPage == NULL) {
        curPageNo = headerPage->firstPage;
        if (curPageNo == -1) return FILEEOF;

        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;

        curDirtyFlag = false;
        status = curPage->firstRecord(tmpRid);
        curRec = tmpRid;

        // checks if there are records on the page
        if (status == NORECORDS)
        {
            // if page 1 is empty, unpin and return FILEEOF
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;
            curPage = NULL;
            curPageNo = -1;
            curDirtyFlag = false;
            return FILEEOF;
        }

        status = curPage->getRecord(tmpRid, rec);
        if (status != OK) return status;

        if (matchRec(rec))
        {
            outRid = tmpRid;
            return OK;
        }
    }

    // otherwise, start where we left off last time, scan pages one record at a time
    while (!recLoop) {
        status = curPage->nextRecord(curRec, nextRid);
        if (status == OK) curRec = nextRid;
        while (status != OK) {
            curPage->getNextPage(nextPageNo);
            if (nextPageNo == -1) return FILEEOF;

            // unpin page, do bookkeeping
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            curPage = NULL;
            curPageNo = -1;
            curDirtyFlag = false;
            if (status != OK) return status;

            curPageNo = nextPageNo;
            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if (status != OK) return status;
            // checks if there are records in the page, if not loops again to next page
            status = curPage->firstRecord(curRec);
        }

        status = curPage->getRecord(tmpRid, rec);
        if (status != OK) return status;

        // use matchRec to see if matched.
        curRec = tmpRid;
        bool recLoop = false;
        if (matchRec(rec)) {
            outRid = tmpRid;
            return OK;
        }
    }

    return status;
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
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
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

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    if (curPage == NULL) {
        // deal with the last page
        curPageNo = headerPage->lastPage;
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        if (status != OK) return status;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if(status != OK) return status;
    }

    status = curPage->insertRecord(rec, outRid);
    if(status == NOSPACE) {
        // create a new page
        // curPage->getNextPage(newPageNo);
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if(status != OK) return status;

        newPage->init(newPageNo);

        status = curPage->setNextPage(newPageNo);
        if(status != OK) return status;

        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        if (status != OK) return status;

        curPage = newPage;
        curPageNo = newPageNo;

        // cout << "getnext page" << endl;
        // status = curPage->getNextPage(newPageNo);
        // if (status != OK) return status;
        
        // curPage = newPage;
        // curPageNo = newPageNo;
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        
        // update the header
        status = newPage->insertRecord(rec, outRid);
        if(status != OK) return status;
    } else if (status != OK) { return status; }

    // cout << "here!\n";
    curRec = outRid;
    outRid.pageNo = curPageNo;
    // cout << "Record page number = " + std::to_string(curRec.pageNo) + "\n";
    // cout << "Record slot number = " + std::to_string(curRec.slotNo) + "\n";
    // cout << "curPageNo = " + std::to_string(curPageNo) + "\n";
    curDirtyFlag = true;
    hdrDirtyFlag = true;
    headerPage->recCnt++; 
    // cout << "Number of pages = " + std::to_string(headerPage->pageCnt) + "\n";
    return OK;}


