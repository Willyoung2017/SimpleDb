package simpledb;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte header[];
    final Tuple tuples[];
    final int numSlots;
    byte[] oldData;
    private final Byte oldDataLock=new Byte((byte)0);
    private TransactionId tid;  // record that transaction that did the dirtying
    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;

        try {
            this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        }catch (NoSuchElementException e){
            System.out.println(e.toString());
            throw new IOException("Invalid page id!");
        }

        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
        this.tid = null;
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {        
        // some code goes here
        if (this.numSlots != 0)
        // has already been set
            return this.numSlots;

        // floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
        return (BufferPool.getPageSize()*8) / (this.td.getSize() * 8 + 1);

    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {        
        
        // some code goes here
        // ceiling(no. tuple slots / 8)
        int headerSz = (getNumTuples() / 8);
        if (headerSz * 8 < getNumTuples())
            headerSz++; //ceiling

        return headerSz;
                 
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    // some code goes here
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if (t.getRecordId() == null || getNumEmptySlots() == this.numSlots || !t.getRecordId().getPageId().equals(pid)){
            throw new DbException("Deletion error.");
        }

        int tupleNo = t.getRecordId().tupleno();

        if (!isSlotUsed(tupleNo)){
            throw new DbException("No need to delete.");
        }

        markSlotUsed(tupleNo, false);

        // update tuple
//        t.setRecordId(null);
        tuples[tupleNo] = null;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if (getNumEmptySlots() == 0) {
            throw new DbException("Insertion error.");
        }

        if (!t.getTupleDesc().equals(td)) {
            throw new DbException("Miss matching.");
        }

        int tupleNo = -1;
        for (int i = 0; i < this.numSlots; ++i){
            if (!(isSlotUsed(i))){
                tupleNo = i;
                markSlotUsed(tupleNo, true);
                break;
            }
        }

        // update tuple
        t.setRecordId(new RecordId(pid, tupleNo));
        tuples[tupleNo] = t;
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	// not necessary for lab1
        if (dirty) {
            this.tid = tid;
        }
        else {
            this.tid = null; // marks as not dirty
        }
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	// Not necessary for lab1
        return this.tid;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int cntEmpty = 0;
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                cntEmpty += 1;
            }
        }
            return cntEmpty;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // e.g. 20 used slot with 32 slot in total, note index i for the 20th slot is 19(0-base)
        // [-1,-1,15,0]
        // [11111111,11111111,00001111,00000000]
        // some code goes here
        int byteIdx = i/8;
        int bitOffset = i%8;
        byte head = this.header[byteIdx];
//        System.out.println("head");
//        System.out.println(head);
//        System.out.println("head_offset");
//        System.out.println(head>>bitOffset);
        return ((head&0x0FF)>>bitOffset)%2==1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
        int byteIdx = i/8;
        int bitOffset = i%8;
        if (isSlotUsed(i) != value)
            header[byteIdx] ^= (1<<bitOffset);
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        return new PageTupleIterator();
    }

    public class PageTupleIterator implements Iterator<Tuple> {
        private int curPos;  //record index in current slots
        private int usedIdx; //record index of used slots
        private int filledSlots;
        @Override
        public Tuple next() {
            if (hasNext()){
                for (int i=curPos; i<tuples.length; ++i){
                    if (isSlotUsed(i)) {
                        curPos = i;
                        break;
                    }
                }
                usedIdx += 1;
                return tuples[curPos++];

            }
            else{
                throw new NoSuchElementException("Has no next element!");
            }
        }

        @Override
        public boolean hasNext() {
            return (usedIdx < this.filledSlots) && (curPos < tuples.length);

//            if (curPos >= tuples.length)
//                return false;
//
//            for (int i=curPos; i<tuples.length; ++i){
//                if (isSlotUsed(i)) {
//                    return true;
//                }
//            }
//            return false;//(usedIdx < tuples.length-getNumEmptySlots()) && (curPos < tuples.length);
        }

        @Override
        public void remove(){
            throw new UnsupportedOperationException();
        }

        private PageTupleIterator(){
            this.curPos = 0;
            this.usedIdx = 0;
            this.filledSlots = numSlots - getNumEmptySlots();
        }

    }


}

