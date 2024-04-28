package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;
    private final Map<PageId, Page> bufferPool;
    private final LockManager lockManager = new LockManager();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // TODO: some code goes here
        this.numPages = numPages;
        this.bufferPool = new LinkedHashMap<PageId, Page>(numPages, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<PageId, Page> eldest) {
                return size() > numPages;
            }
        };
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
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        try {
            if (!lockManager.acquireLock(tid, pid, perm, 0)) {
                throw new TransactionAbortedException();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransactionAbortedException();
        }

        Page page = bufferPool.get(pid);
        if (page != null) {
            bufferPool.remove(pid);
            bufferPool.put(pid, page);
            return page;
        }
        if (bufferPool.size() >= numPages) {
            evictPage();
        }
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        page = dbFile.readPage(pid);
        bufferPool.put(pid, page);
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
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            rollBack(tid);
        }
    }

    public synchronized void rollBack(TransactionId tid) {
        Set<PageId> pids = new HashSet<>(bufferPool.keySet());
        for (PageId pid : pids) {
            Page page = bufferPool.get(pid);
            if (tid.equals(page.isDirty())) {
                int tableId = pid.getTableId();
                DbFile table = Database.getCatalog().getDatabaseFile(tableId);
                Page readPage = table.readPage(pid);
                bufferPool.remove(pid, page);
                bufferPool.put(pid, readPage);
            }
        }
        lockManager.lockTable.keySet().removeIf(pid -> lockManager.holdsLock(tid, pid));
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = dbFile.insertTuple(tid, t);
        for (Page page : pages) {
            page.markDirty(true, tid);
            if (!bufferPool.containsKey(page.getId())) {
                if (bufferPool.size() >= numPages) {
                    evictPage();
                }
                bufferPool.put(page.getId(), page);
            }
        }        
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        RecordId recordId = t.getRecordId();
        PageId pageId = recordId.getPageId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pageId.getTableId());
        List<Page> pages = dbFile.deleteTuple(tid, t);
        for (Page page : pages) {
            page.markDirty(true, tid);
            if (!bufferPool.containsKey(page.getId())) {
                if (bufferPool.size() >= numPages) {
                    evictPage();
                }
                bufferPool.put(page.getId(), page);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        Set<PageId> pids = new HashSet<>(bufferPool.keySet());
        for (PageId pid : pids) {
            flushPage(pid);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1
        bufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        Page page = bufferPool.get(pid);
        if (page == null) {
            throw new IOException("Page not found in buffer pool");
        }
        if (page.isDirty() != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        Set<PageId> pids = new HashSet<>(bufferPool.keySet());
        for (PageId pid : pids) {
            Page page = bufferPool.get(pid);
            if (tid.equals(page.isDirty())) {
                flushPage(pid);
            }
        }
        lockManager.lockTable.keySet().removeIf(pid -> lockManager.holdsLock(tid, pid));
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // TODO: some code goes here
        // not necessary for lab1
        Set<PageId> pids = new HashSet<>(bufferPool.keySet());
        for (PageId pid : pids) {
            if (bufferPool.get(pid).isDirty() == null) {
                bufferPool.remove(pid);
                return;
            }
        }
        throw new DbException("All pages are dirty");
    }

    private class LockManager {
        private final ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> lockTable;

        public LockManager() {
            lockTable = new ConcurrentHashMap<>();
        }

        public class PageLock {
            private final PageId pid;
            private final TransactionId tid;
            private final Permissions perm;

            public PageLock(PageId pid, TransactionId tid, Permissions perm) {
                this.pid = pid;
                this.tid = tid;
                this.perm = perm;
            }
        }

        public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions perm, int retry)
                throws TransactionAbortedException, InterruptedException {
            if (retry == 3) {
                return false;
            }
            ConcurrentHashMap<TransactionId, PageLock> locks = lockTable.get(pid);
            if (locks == null) {
                locks = new ConcurrentHashMap<>();
                PageLock lock = new PageLock(pid, tid, perm);
                locks.put(tid, lock);
                lockTable.put(pid, locks);
                return true;
            }
            
            if (locks.get(tid) == null) {
                if (perm == Permissions.READ_WRITE) {
                    wait(100);
                    return acquireLock(tid, pid, perm, retry + 1);
                } else if (perm == Permissions.READ_ONLY) {
                    if (locks.size() > 1) {
                        PageLock lock = new PageLock(pid, tid, perm);
                        locks.put(tid, lock);
                        return true;
                    } else {
                        Collection<PageLock> values = locks.values();
                        for (PageLock lock : values) {
                            if (lock.perm == Permissions.READ_WRITE) {
                                wait(100);
                                return acquireLock(tid, pid, perm, retry + 1);
                            } else {
                                PageLock newLock = new PageLock(pid, tid, perm);
                                locks.put(tid, newLock);
                                return true;
                            }
                        }
                    }
                }
            } else {
                if (perm == Permissions.READ_ONLY) {
                    locks.remove(tid);
                    PageLock lock = new PageLock(pid, tid, perm);
                    locks.put(tid, lock);
                    return true;
                } else {
                    if (locks.get(tid).perm == Permissions.READ_WRITE) {
                        return true;
                    } else {
                        if (locks.size() > 1) {
                            wait(100);
                            return acquireLock(tid, pid, perm, retry + 1);
                        } else {
                            locks.remove(tid);
                            PageLock lock = new PageLock(pid, tid, perm);
                            locks.put(tid, lock);
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        public synchronized void releaseLock(TransactionId tid, PageId pid) {
            if (holdsLock(tid, pid)) {
                ConcurrentHashMap<TransactionId, PageLock> locks = lockTable.get(pid);
                locks.remove(tid);
                if (locks.isEmpty()) {
                    lockTable.remove(pid);
                }
                this.notifyAll();
            }
        }

        public boolean holdsLock(TransactionId tid, PageId pid) {
            if (lockTable.get(pid) == null) {
                return false;
            }
            return lockTable.get(pid).get(tid) != null;
        }
    }
}
