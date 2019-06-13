package simpledb;

import java.io.*;
import java.util.*;
import java.lang.reflect.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class LockManager {
    private ConcurrentHashMap<PageId, Object> locks;
    private ConcurrentHashMap<PageId, List<TransactionId>> sharedLocks;
    private ConcurrentHashMap<PageId, TransactionId> exclusiveLocks;
    private ConcurrentHashMap<TransactionId, List<PageId>> tid2PageId;
    private ConcurrentHashMap<TransactionId, LinkedBlockingQueue<TransactionId>> depGraph;
    private ConcurrentHashMap<TransactionId, LinkedBlockingQueue<PageId>> exlockTid2PageId;

    public LockManager() {
        sharedLocks = new ConcurrentHashMap<>();
        exclusiveLocks = new ConcurrentHashMap<>();
        tid2PageId = new ConcurrentHashMap<>();
        locks = new ConcurrentHashMap<>();
        depGraph = new ConcurrentHashMap<>();
        exlockTid2PageId = new ConcurrentHashMap<>();
    }

    private boolean waitforReadPerm(TransactionId tid, PageId pid) throws TransactionAbortedException {
        locks.putIfAbsent(pid, new Object());
        Object lock = locks.get(pid);
        while(true) {
            // use synchronized to prevent request at the same time
            synchronized(lock){
                TransactionId holder = exclusiveLocks.get(pid);
                // if no exclusive lock on the pid or tid is the exclusive lock holder
                if (holder == null || holder.equals(tid)){
                    depGraph.remove(tid);
                    sharedLocks.putIfAbsent(pid, new ArrayList<>());
                    sharedLocks.get(pid).add(tid);
                    return true;
                }
//                if (tid == null) System.out.println("h111hhh");

                depGraph.putIfAbsent(tid, new LinkedBlockingQueue<>());
                LinkedBlockingQueue<TransactionId> dependees = depGraph.get(tid);
                // check if a new dependee is added
                if (!dependees.contains(holder) && !holder.equals(tid)) {
                    dependees.add(holder);
                    detectDeadLock(tid);
                }

            }
        }
    }

    private void detectDeadLock(TransactionId tid) throws TransactionAbortedException {
        Set<TransactionId> visitedId = new HashSet<>();
        for (TransactionId tidd : depGraph.keySet()) {
            if (!visitedId.contains(tidd))
                checkDeadLock(tidd, visitedId, new Stack<>());
        }
    }

    private void checkDeadLock(TransactionId tid, Set<TransactionId> visitedId, Stack<TransactionId> dependents)  throws TransactionAbortedException {

        visitedId.add(tid);
//        if (tid == null) System.out.println("hhhhhnill");
        if (!depGraph.containsKey(tid)) return;
        LinkedBlockingQueue<TransactionId> dependees = depGraph.get(tid);
        for (Iterator<TransactionId> iterator = dependees.iterator(); iterator.hasNext(); ){
            TransactionId dependee = iterator.next();
//        for (TransactionId dependee : dependees) {
            // tid's dependee appears in the list of tid's dependents
            if (dependents.contains(dependee)) {
                throw new TransactionAbortedException();
            }
            // check tid's dependee and tid become member of dependents this time
            if (!visitedId.contains(dependee)) {
                dependents.push(tid);
                checkDeadLock(dependee, visitedId, dependents);
                dependents.pop();
            }
        }
    }

    private boolean waitforWritePerm(TransactionId tid, PageId pid) throws TransactionAbortedException {
        locks.putIfAbsent(pid, new Object());
        Object lock = locks.get(pid);
        while(true) {
            // use synchronized to prevent request at the same time
            synchronized(lock){
                TransactionId holder = exclusiveLocks.get(pid);
                List<TransactionId> holderList = sharedLocks.get(pid);

                if (!((holder!=null && !holder.equals(tid)) || (holderList!=null && ((holderList.size()==1 && !holderList.get(0).equals(tid)) || (holderList.size()>1))))) {
                    depGraph.remove(tid);
                    exclusiveLocks.put(pid, tid);
                    exlockTid2PageId.putIfAbsent(tid, new LinkedBlockingQueue<>());
                    exlockTid2PageId.get(tid).add(pid);
                    return true;
                }
//                if (tid == null) System.out.println("h222hhh");

                depGraph.putIfAbsent(tid, new LinkedBlockingQueue<>());
                LinkedBlockingQueue<TransactionId> dependees = depGraph.get(tid);
                boolean flag = false;
                // check if a new dependee is added
                if (holder!= null && !dependees.contains(holder) && !holder.equals(tid)) {
                    dependees.add(holder);
                    flag = true;
                }

                if (holderList != null) {
                    for (TransactionId holderId : holderList) {
                        if (!dependees.contains(holderId) && !holderId.equals(tid)) {
                            dependees.add(holderId);
                            flag = true;
                        }
                    }
                }

                if(flag) {
                    detectDeadLock(tid);
                }
            }
        }
    }

    public boolean requestLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
//        TransactionId tran_id = tid;
        if (pid == null) System.out.println("It's not my fault!");
        if (tid == null) {
            throw new TransactionAbortedException();
        }

        if (perm == Permissions.READ_ONLY) {
            if (sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid)) {
                return true;
            }
            while(true){
                if(waitforReadPerm(tid, pid))
                    break;
            }
//            synchronized (exclusiveLocks) {
//                if (exclusiveLocks.containsKey(pid)) {
//                    if (pid == null) System.out.println("pid is Null!");
//                    TransactionId tt = exclusiveLocks.get(pid);
//                    if (tt == null) System.out.println("tt is Null!");
//
//                    if (tt.equals(tid)) {
//                        return true;
//                    }
//                }
//            }
//            while(true){
//                if(waitforWritePerm(tid, pid))
//                    break;
//            }
        }
        else if (perm == Permissions.READ_WRITE) {
//            synchronized (exclusiveLocks) {
            if (exclusiveLocks.containsKey(pid) && exclusiveLocks.get(pid).equals(tid)) {
//                    if (pid == null) System.out.println("pid is Null!");
//                TransactionId tt =
////                    if (tt == null) System.out.println("tt is Null!");
//
//                if (tt.equals(tid)) {
//                    return true;
//                }
                return true;
            }
//            }
            while(true){
                if(waitforWritePerm(tid, pid))
                    break;
            }
        }

        // the page has not been requested yet
        tid2PageId.putIfAbsent(tid, new ArrayList<>());
        tid2PageId.get(tid).add(pid);
        return true;

    }

    public boolean holdsLock(TransactionId tid, PageId p) {
        return (tid2PageId.containsKey(tid) && tid2PageId.get(tid).contains(p));
    }

    public void releasePage(TransactionId tid, PageId pid) {
        if (holdsLock(tid, pid)){
            Object lock = locks.get(pid);
            synchronized(lock) {
                exclusiveLocks.remove(pid);
                sharedLocks.getOrDefault(pid, new ArrayList<>()).remove(tid);
            }
            tid2PageId.get(tid).remove(pid);
            exlockTid2PageId.remove(tid);

        }
    }

    public void releasePages(TransactionId tid) {
        if (tid2PageId.containsKey(tid)) {
            List<PageId> pageIdList = tid2PageId.get(tid);
            for (PageId pid : pageIdList) {
                Object lock = locks.get(pid);
                synchronized(lock) {
                    exclusiveLocks.remove(pid);
                    sharedLocks.getOrDefault(pid, new ArrayList<>()).remove(tid);
                    depGraph.remove(tid);
                }
            }
            tid2PageId.remove(tid);
            exlockTid2PageId.remove(tid);
        }
    }

    public ConcurrentHashMap<TransactionId, LinkedBlockingQueue<PageId>> getExlockTid2PageId() {
        return exlockTid2PageId;
    }
}
