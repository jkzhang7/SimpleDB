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
	
	private File file;
	private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.file = f;
    	this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
    	
        return this.file;
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
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
    	return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
    	return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
//        return null;

    	int pageSize = BufferPool.getPageSize();
    	int pageNum = pid.getPageNumber();
    	byte[] data = new byte[pageSize];
    	
    	try {
    		RandomAccessFile f = new RandomAccessFile(file,"r");
    		f.seek(pageNum * pageSize);
    		f.read(data, 0, pageSize);
    		HeapPageId id = new HeapPageId(pid.getTableId(),pid.getPageNumber());
    		Page page = new HeapPage(id, data);
    		return page;
    	} catch (IOException e) {
            e.printStackTrace();
    	}	
    	return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here

        int res = (int)Math.floor(file.length() / BufferPool.getPageSize());
        return res;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {

        // some code goes here
//        return null;
    	return new HeapFileIterator(tid);
    }
    
    private class HeapFileIterator implements DbFileIterator{
    	
    	private Iterator<Tuple> tupleit;
    	private int page_num;
    	private TransactionId tid;
   
  
    	public HeapFileIterator(TransactionId tid) {
			this.tid = tid;
        }
    	
        public Iterator<Tuple> getTuple(HeapPageId pid) throws TransactionAbortedException, DbException {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        }
    
        @Override
        public void open() throws DbException, TransactionAbortedException {
            	page_num = 0;
                HeapPageId pid = new HeapPageId(getId(),page_num);
                tupleit = getTuple(pid);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
        	if (tupleit == null) {
        		return false;
        	}             
        	if (tupleit.hasNext()) {
                return true;
            }
            if (page_num < numPages() - 1) {
                page_num++;
                HeapPageId pid = new HeapPageId(getId(), page_num);
                tupleit = getTuple(pid);
                return tupleit.hasNext();
            } else return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        	 if(tupleit == null || !tupleit.hasNext()){
                 throw new NoSuchElementException();
             }
             return tupleit.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
            close();
        }

        @Override
        public void close() {
        	tupleit = null;
        }
    	
    	
    };
}