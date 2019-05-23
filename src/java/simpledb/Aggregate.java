package simpledb;

import com.sun.deploy.security.ValidationState;

import java.util.*;

import static simpledb.Aggregator.NO_GROUPING;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private boolean open = false;
    private TupleDesc childTupleDesc;
    private Aggregator aggregator;
    private TupleIterator outputIter = null;
    private Type gbfieldType;
    private Type afieldType;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.child = child;
        this.childTupleDesc = child.getTupleDesc();
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.gbfieldType = this.gfield == NO_GROUPING ? null: this.childTupleDesc.getFieldType(this.gfield);
        this.afieldType = this.childTupleDesc.getFieldType(this.afield);

        if (this.afieldType == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(gfield, this.gbfieldType, afield, aop);
        }
        else if (this.afieldType == Type.STRING_TYPE) {
            this.aggregator = new StringAggregator(gfield, this.gbfieldType, afield, aop);
        }

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	    return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
	    return this.gfield == NO_GROUPING ? null:childTupleDesc.getFieldName(this.gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	return this.childTupleDesc.getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
        super.open();
        this.child.open();
        this.open = true;
        while (this.child.hasNext()){
            this.aggregator.mergeTupleIntoGroup(this.child.next());
        }
        this.outputIter = (TupleIterator) this.aggregator.getTupleIter(getTupleDesc());
        this.outputIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        if (this.outputIter.hasNext())
            return this.outputIter.next();
	    return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        if (this.open){
            this.outputIter.rewind();
        }
        else
            throw new DbException("File is closed");
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
        TupleDesc outputDesc;
        if (this.gfield != NO_GROUPING)
            outputDesc = new TupleDesc(new Type[] {this.gbfieldType,Type.INT_TYPE}, new String[]{groupFieldName(), this.aop.toString() + "(" + aggregateFieldName() + ")"});
	    else
	        outputDesc = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[]{this.aop.toString() + "(" + aggregateFieldName() + ")"});

    return outputDesc;
    }

    public void close() {
	// some code goes here
        super.close();
        this.child.close();
        this.open = false;
        this.outputIter = null;
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
