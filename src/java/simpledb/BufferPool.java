package simpledb;

import java.io.*;

import java.security.Permission;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    private ConcurrentHashMap<PageId, Page> id2Page;

    private LockManager lockManager;

//    private ConcurrentHashMap<TransactionId, LinkedBlockingQueue<PageId>> tid2PageId;
    /* max number of pages */
    public final int MAX_NUM_PAGES;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.MAX_NUM_PAGES = numPages;
        this.id2Page = new ConcurrentHashMap<>();
        this.lockManager = new LockManager();
//        this.tid2PageId = new ConcurrentHashMap<>();
    }

    public static int getPageSize() {
      return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
//        TransactionId curTransId = id2TId.get(pid);
//
//        // check transaction id
//        if (curTransId != null && holdsLock(curTransId, pid)){
//            throw new TransactionAbortedException();
//        }
//        if (tid == null) System.out.println("NUlllllll!!!");

        lockManager.requestLock(tid, pid, perm);
        Page page = id2Page.get(pid);
        if (page == null) {

            int tableId = pid.getTableId();
            try {
                DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
                page = dbFile.readPage(pid);
            } catch (RuntimeException e){
                System.out.println(e.toString());
                throw new DbException("Cannot find dbFile or page!");
            }

            /* add page to the buffer pool */
            if (!id2Page.containsKey(pid) && id2Page.size() >= this.MAX_NUM_PAGES){
                evictPage();
                // throw new DbException("Insufficient space left in buffer pool!");
            }
            if (page == null) System.out.println("get page null");

            id2Page.put(pid, page);
        }

        // update transaction id
//        id2TId.put(pid, tid);

        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
//        System.out.println("releasing"+pid.toString());
        lockManager.releasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        // if commit, flush dirty pages associated to the transaction to disk
        // if abort, revert any changes made by the transaction by restoring the page to its on-disk state
//        if (commit) {
//            flushPages(tid);
//        }
//        else {
//
//        }
        ConcurrentHashMap<TransactionId, LinkedBlockingQueue<PageId>> tid2PageId = lockManager.getExlockTid2PageId();
        if (!tid2PageId.containsKey(tid)) {
//            System.out.println("Problems occur!");
            lockManager.releasePages(tid);

            return;
        }
        LinkedBlockingQueue<PageId> pageIdlist = tid2PageId.get(tid);

        if (commit) {
            for (PageId pid : pageIdlist) {
//                Page p = id2Page.get(pid);
//                flushPage();
                flushPage(pid);
//                id2Page.get(pid).setBeforeImage();
                try {
                    getPage(tid,pid,Permissions.READ_WRITE).setBeforeImage();
                }
                catch (TransactionAbortedException e) {
                    e.printStackTrace();
                } catch (DbException e) {
                    e.printStackTrace();
                }
            }
        }
        else {
            for (PageId pid : pageIdlist) {
                Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                id2Page.replace(pid, page);
                page.markDirty(false, null);
            }
        }

//        if (commit) {
////            flushPages(tid);
////            LinkedBlockingQueue<Pa>
////            for (: tid2PageId.)
//            synchronized (id2Page) {
//                for (PageId pid : id2Page.keySet()) {
//                    Page p = id2Page.get(pid);
//                    if (p == null) System.out.println("p is Null!" + id2Page.containsKey(pid) + pid.toString());
//                    if (tid.equals(p.isDirty())) {
//                        flushPage(pid);
//                        try {
//                            getPage(tid,pid,Permissions.READ_WRITE).setBeforeImage();
//                        } catch (TransactionAbortedException e) {
//                            e.printStackTrace();
//                        } catch (DbException e) {
//                            e.printStackTrace();
//                        }
////                        p.setBeforeImage();
//
//                    }
//                }
//            }
//        }
//        else {
//            FileWriter writee = new FileWriter("the-file-name.txt", true);
//            writee.write("Reseting..."+tid.toString()+"\n");
//            writee.close();
////            System.out.println("Reseting..."+tid.toString());
//            synchronized (id2Page) {
//                for (PageId pid : id2Page.keySet()) {
//                    Page p = id2Page.get(pid);
//
//                    if (tid.equals(p.isDirty())) {
//                        if (p.getBeforeImage() == null) System.out.println("setimage null");
////                        p.setBeforeImage();
//                        Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
//                        id2Page.replace(pid, page);
////                        System.out.println("Reset" + p.toString() + "to" + page.toString());
//                        FileWriter wriee = new FileWriter("the-file-name.txt", true);
//                        wriee.write("Reset" + p.toString() + "to" + page.toString()+"\n");
//                        wriee.close();
//                        page.markDirty(false, null);
//                    }
//                }
//            }
//        }

        // release any state the BufferPool keeps regarding the transaction, including releasing any locks that the transaction held
        lockManager.releasePages(tid);

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        try {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
            ArrayList<Page> dirtyPages = dbFile.insertTuple(tid, t);
            // may increase pages due to insert
            for (Page p:dirtyPages) {
                if (!id2Page.containsKey(p.getId()) && id2Page.size()==MAX_NUM_PAGES){
                    evictPage();
                }
                id2Page.put(p.getId(), p);
    //                id2TId.put(p.getId(), tid);
                // Marks any pages that were dirtied
//                System.out.println(p.toString()+" is dirtied by "+tid.toString());
                p.markDirty(true, tid);
//                tid2PageId.putIfAbsent(tid, new LinkedBlockingQueue<>());
//                LinkedBlockingQueue<PageId> pageIdList = tid2PageId.get(tid);
//                pageIdList.add(p.getId());
            }
        } catch (RuntimeException e){
            System.out.println(e.toString());
            throw new DbException("Cannot find dbFile or page!");
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        try {
            int tableId = t.getRecordId().getPageId().getTableId();
            DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
            ArrayList<Page> dirtyPages = dbFile.deleteTuple(tid, t);
//            FileWriter ww = new FileWriter("the-file-name.txt", true);
//            ww.write("A size of" +dirtyPages.size()+"is dirtied by "+tid.toString()+" in deletion.\n");
//            ww.close();
            for (Page p:dirtyPages) {
                if (!id2Page.containsKey(p.getId()) && id2Page.size()==MAX_NUM_PAGES){
                    evictPage();
                }

    //                id2TId.put(p.getId(), tid);

                // Marks any pages that were dirtied
                p.markDirty(true, tid);
                id2Page.put(p.getId(), p);
//                FileWriter writer = new FileWriter("the-file-name.txt", true);
//                writer.write(p.toString()+" is dirtied by "+tid.toString()+" in deletion.\n");
//                writer.close();
//
//                tid2PageId.putIfAbsent(tid, new LinkedBlockingQueue<>());
//                LinkedBlockingQueue<PageId> pageIdList = tid2PageId.get(tid);
//                pageIdList.add(p.getId());
            }
        } catch (RuntimeException e){
            System.out.println(e.toString());
            throw new DbException("Cannot find dbFile or page!");
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Iterator<PageId> id2PageIter = id2Page.keySet().iterator();

        while(id2PageIter.hasNext()){
            PageId thisPageId = id2PageIter.next();
            flushPage(thisPageId);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
//        synchronized (id2Page) {

        id2Page.remove(pid);
//        }
//        id2TId.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page disPage = id2Page.get(pid);
        if (disPage != null && disPage.isDirty() != null){
            int tableId = pid.getTableId();
            DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
            // Push the specified page to disk.
            dbFile.writePage(disPage);
            disPage.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : id2Page.keySet()) {
            Page p = id2Page.get(pid);
            if (tid.equals(p.isDirty()))
                flushPage(pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Page dispage = null;
//        LinkedBlockingQueue<PageId> candPageId = new LinkedBlockingQueue<>();
        List<PageId> candPageId = new LinkedList<>();
        Iterator<PageId> id2PageIter = id2Page.keySet().iterator();
        while(id2PageIter.hasNext()){

            PageId thisPageId = id2PageIter.next();

            dispage = id2Page.get(thisPageId);

            // do not evict dirty pages NO STEAL
            if (dispage.isDirty()==null){
                candPageId.add(thisPageId);
            }
        }

        if (candPageId.size() == 0) {
            throw new DbException("No suitable page to evict!");
        }

        Random rand = new Random();
        PageId chosenPageId = candPageId.get(rand.nextInt(candPageId.size()));
//        PageId chosenPageId = candPageId.peek();

        try {
            Page chosenPage = id2Page.get(chosenPageId);
            assert chosenPage.isDirty() == null;
            flushPage(chosenPageId);
        }
        catch (IOException e){
            System.out.println(e.toString());
            throw new DbException("Error exists in pages!");
        }
//        System.out.println("evicting"+chosenPageId.toString());
//        synchronized (id2Page) {
        id2Page.remove(chosenPageId);
//        }
//        id2TId.remove(chosenPageId);
    }

}
