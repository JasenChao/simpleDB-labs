package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;

    private static final class HeapFileIterator implements DbFileIterator {
        private final TransactionId tid;
        private final HeapFile heapFile;
        private Iterator<Tuple> tupleIterator;
        private int currentPageNo;

        public HeapFileIterator(TransactionId tid, HeapFile heapFile) {
            this.tid = tid;
            this.heapFile = heapFile;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            currentPageNo = 0;
            tupleIterator = getTupleIterator(currentPageNo);
        }

        private Iterator<Tuple> getTupleIterator(int pageNo) throws TransactionAbortedException, DbException {
            if (pageNo >= 0 && pageNo < heapFile.numPages()) {
                HeapPageId pid = new HeapPageId(heapFile.getId(), pageNo);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            } else {
                throw new DbException(String.format("heapFile %d doesn't exist in page[%d]", pageNo, heapFile.getId()));
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (tupleIterator == null) {
                return false;
            }
            if (tupleIterator.hasNext()) {
                return true;
            }
            if (currentPageNo < heapFile.numPages() - 1) {
                currentPageNo++;
                tupleIterator = getTupleIterator(currentPageNo);
                return this.hasNext();
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException {
            if (tupleIterator == null || !tupleIterator.hasNext()) {
                throw new NoSuchElementException();
            }
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            tupleIterator = null;
            currentPageNo = 0;
        }
    }

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // TODO: some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // TODO: some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // TODO: some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // TODO: some code goes here
        int tableId = pid.getTableId();
        int pageNo = pid.getPageNumber();
        int offset = pageNo * BufferPool.getPageSize();
        RandomAccessFile randomAccessFile = null;

        try {
            randomAccessFile = new RandomAccessFile(file, "r");
            if ((long) (pageNo + 1) * BufferPool.getPageSize() > randomAccessFile.length()) {
                randomAccessFile.close();
                throw new IllegalArgumentException("pageNo is out of file length");
            }
            byte[] data = new byte[BufferPool.getPageSize()];
            randomAccessFile.seek(offset);
            int read = randomAccessFile.read(data, 0, BufferPool.getPageSize());
            if (read != BufferPool.getPageSize()) {
                throw new IllegalArgumentException("read page failed");
            }
            HeapPageId id = new HeapPageId(tableId, pageNo);
            return new HeapPage(id, data);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (randomAccessFile != null) {
                    randomAccessFile.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException("read page failed");
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        PageId pageId = page.getId();
        int pageNo = pageId.getPageNumber();
        int offset = pageNo * BufferPool.getPageSize();
        byte[] pageData = page.getPageData();

        RandomAccessFile file = new RandomAccessFile(this.file, "rw");
        file.seek(offset);
        file.write(pageData);
        file.close();

        page.markDirty(false, null);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // TODO: some code goes here
        return (int) Math.floor(getFile().length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        ArrayList<Page> pages = new ArrayList<Page>();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumUnusedSlots() == 0) {
                Database.getBufferPool().unsafeReleasePage(tid, pid);
                continue;
            }
            page.insertTuple(t);
            pages.add(page);
            return pages;
        }

        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(file, true));
        byte[] emptyData = HeapPage.createEmptyPageData();
        bw.write(emptyData);
        bw.close();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), numPages() - 1), Permissions.READ_WRITE);
        page.insertTuple(t);
        page.markDirty(true, tid);
        pages.add(page);
        return pages;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);
        return Collections.singletonList(page);
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // TODO: some code goes here
        return new HeapFileIterator(tid, this);
    }

}

