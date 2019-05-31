package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File backingFile;
    private TupleDesc fileTd;
    private RandomAccessFile randAccessFile;

    public HeapFile(File f, TupleDesc td) throws RuntimeException{
        // some code goes here
        this.backingFile = f;
        this.fileTd = td;
        try{
            this.randAccessFile = new RandomAccessFile(this.backingFile, "rw");
        }catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.backingFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.backingFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.fileTd;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pgNo = pid.pageNumber();
        int pgSz = BufferPool.getPageSize();
        HeapPage page;

        // pgNo is 0-base, just use to calc offset
        int offset = pgNo * pgSz;
        byte[] data = new byte[pgSz];
        try{
            this.randAccessFile.seek(offset);
            this.randAccessFile.read(data,0,pgSz);
            page = new HeapPage((HeapPageId) pid, data);
        }catch (IOException e) {
            throw new RuntimeException(e);
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        int pgNo = pid.pageNumber();
//        System.out.println(pgNo);
        int pgSz = BufferPool.getPageSize();
        int offset = pgSz * pgNo;
        try{
            this.randAccessFile.seek(offset);
            this.randAccessFile.write(page.getPageData());
        }catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return  (int) Math.ceil((double)this.backingFile.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        HeapPage insertedPage = null;
        ArrayList<Page> dirtyPage = new ArrayList<>();

        int pageNumber = 0;
        while (pageNumber < numPages()) {
            HeapPageId pid = new HeapPageId(getId(), pageNumber);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if (page.getNumEmptySlots()>0){
                // find page with empty slots
                page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                insertedPage = page;
                insertedPage.insertTuple(t);
                dirtyPage.add(insertedPage);
                return dirtyPage;
            }
            pageNumber ++;
            // may need to release page here.

        }

//        System.out.println("hhh");
        // no page to insert
        HeapPageId newPid = new HeapPageId(getId(), numPages());
        HeapPage newPage = new HeapPage(newPid, HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        writePage(newPage);

        dirtyPage.add(newPage);

        return dirtyPage;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        if (pid == null)
            throw new DbException("No page found.");

        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        ArrayList<Page> dirtyPage = new ArrayList<>();
        dirtyPage.add(page);

        return dirtyPage;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
//        return new HeapFileIterator(tid, getId(), numPages());
        return new HeapFileItertor(tid);
    }

    public class HeapFileItertor implements DbFileIterator {
        private boolean is_open;
        private int curPgNum;
        private Iterator<Tuple> curPgIterator;
        private TransactionId tid;
        private Tuple next;
        public void open()
                throws DbException, TransactionAbortedException{
            if (this.is_open){
                throw new DbException("File is open!");
            }
            this.curPgNum = 0;
            this.curPgIterator = getPgIter(curPgNum);
            this.is_open = true;
//            this.next = get_next();
        }

        public boolean hasNext()
                throws DbException, TransactionAbortedException{
            if (!this.is_open){
                return false;
            }
//            return this.next != null;
            if (this.curPgIterator.hasNext())
                return true;

            this.curPgNum ++;
            if (this.curPgNum < numPages()){
                this.curPgIterator = getPgIter(this.curPgNum);
                return this.curPgIterator.hasNext();

            }
            return false;
        }

//        private Tuple get_next()
//                throws DbException, TransactionAbortedException, NoSuchElementException{
//            Tuple nextTuple = null;
//
//            while (this.curPgNum < numPages()) {
//                if (this.curPgIterator.hasNext()){
//                    nextTuple = this.curPgIterator.next();
//                    break;
//                }
//                // prev page has no element and go for nxt page
//                this.curPgNum += 1;
//                if (this.curPgNum >= numPages()){
//                    break;
//                }
//                this.curPgIterator = getPgIter(this.curPgNum);
//            }
//            if (this.curPgIterator.hasNext()){
//                nextTuple = this.curPgIterator.next();
//            }
//            // prev page has no element and go for nxt page
//            this.curPgNum += 1;
//            if (this.curPgNum < numPages()){
//                this.curPgIterator = getPgIter(this.curPgNum);
//                nextTuple = this.curPgIterator.next();
//            }
//
//            // nxtTuple may be null here
//            return nextTuple;
//        }

        public Tuple next()
                throws DbException, TransactionAbortedException, NoSuchElementException{
            if (!this.is_open || !hasNext()){
                throw new NoSuchElementException();
            }
//            Tuple nextTuple = this.next;
//            this.next = get_next();

            return this.curPgIterator.next();
        }

        public void rewind() throws DbException, TransactionAbortedException{
            if (this.is_open) {
                close();
                open();
            }
            else {
                throw new DbException("File is closed!");
            }
        }

        public void close(){
            this.is_open = false;
        }

        public HeapFileItertor(TransactionId tid){
            this.tid = tid;
        }

        private Iterator<Tuple> getPgIter(int pgNum) throws DbException,
                TransactionAbortedException{
            // generate tableid using getId() i.e. the heap file id
            HeapPageId pid = new HeapPageId(getId(), pgNum);
            HeapPage hp = (HeapPage) Database.getBufferPool().getPage(this.tid, pid, Permissions.READ_ONLY);

            return hp.iterator();
        }
    }


}

