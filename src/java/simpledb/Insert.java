package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;
    private int tableId;
    private boolean open;
    private boolean is_inserted = false;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.t = t;
        this.child = child;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"NumberInserted"});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.open = true;
        this.is_inserted = false;
        super.open();
        this.child.open();
    }

    public void close() {
        // some code goes here
        this.open = false;
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        if (!this.open){
            throw new DbException("File is closed.");
        }
        this.close();
        this.open();
        this.child.rewind();

    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.is_inserted)
            return null;
        int count = 0;
        while (this.child.hasNext()){
            Tuple nextTuple = this.child.next();
            try {
                Database.getBufferPool().insertTuple(this.t, this.tableId, nextTuple);
            }
            catch (IOException e){
                throw new DbException("Delete error.");
            }
            count ++;
        }
        Tuple output = new Tuple(getTupleDesc());
        output.setField(0, new IntField(count));
        this.is_inserted = true;
        return output;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] children = {this.child};
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child = children[0];

    }
}
