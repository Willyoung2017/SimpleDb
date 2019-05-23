package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p;
    private DbIterator child1;
    private DbIterator child2;
    private boolean open = false;
    private Tuple nextTuple1 = null;
    private TupleDesc jointTupleDesc = null;
    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        this.jointTupleDesc = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(this.p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().getFieldName(this.p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.jointTupleDesc;
    }

    private boolean is_matched(Tuple tuple1, Tuple tuple2){
        return this.p.filter(tuple1, tuple2);
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.child1.open();
        this.child2.open();
        this.open = true;
        this.nextTuple1 = null;
    }

    public void close() {
        // some code goes here
        this.child1.close();
        this.child2.close();
        super.close();
        this.open = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        if (this.open){
            this.child1.rewind();
            this.child2.rewind();
            this.nextTuple1 = null;
        }
        else
            throw new DbException("File is closed!");
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while(this.child1.hasNext() || this.child2.hasNext()){
            while(this.nextTuple1 != null && this.child2.hasNext()){
                Tuple nextTuple2 = this.child2.next();
                if (is_matched(this.nextTuple1, nextTuple2))
                    return Tuple.joinTuples(this.nextTuple1, nextTuple2, this.jointTupleDesc);
            }
            if (!this.child1.hasNext()) return null;
            this.child2.rewind();
            this.nextTuple1 = this.child1.next();
        }
        return null;

    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] children = {this.child1, this.child2};

        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }

}
