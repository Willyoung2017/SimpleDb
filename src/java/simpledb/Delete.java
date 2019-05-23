package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;
    private boolean open;
    private boolean is_deleted = false;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.t = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"NumberDeleted"});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.open = true;
        this.is_deleted = false;
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
        this.child.rewind();
        this.close();
        this.open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    public Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.is_deleted)
            return null;

        int count = 0;
        while (this.child.hasNext()){
            Tuple nextTuple = this.child.next();
            count ++;

            try {
                Database.getBufferPool().deleteTuple(this.t, nextTuple);
            }
            catch (IOException e){
                throw new DbException("Delete error.");
            }
        }
//        System.out.println("hhh"+count);
        Tuple output = new Tuple(getTupleDesc());
        output.setField(0, new IntField(count));
        this.is_deleted = true;

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
