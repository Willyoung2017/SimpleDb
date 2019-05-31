package simpledb;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final  StringField NO_GROUPING_FIELD = new StringField("NO_GROUPING", 11);
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ConcurrentHashMap<Field, Integer> field2Value = new ConcurrentHashMap<>();
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) throws IllegalArgumentException{
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        if (what != Op.COUNT)
            throw new IllegalArgumentException("Op is not COUNT.");

        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field tupField;
        if (this.gbfield == NO_GROUPING)
            tupField = NO_GROUPING_FIELD;
        else
            tupField = tup.getField(this.gbfield);
//        System.out.println(tupField.toString());
        Integer countValue = field2Value.getOrDefault(tupField, 0);

        countValue += 1;
        field2Value.put(tupField, countValue);
//        System.out.println(tupField+"count"+countValue.toString());

    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        TupleDesc outputDesc;
        if (this.gbfield != NO_GROUPING)
            outputDesc = new TupleDesc(new Type[] {this.gbfieldtype, Type.INT_TYPE}, new String[]{"GROUPED BY", this.what.toString()});
        else
            outputDesc = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[]{this.what.toString()});

        return getTupleIter(outputDesc);

    }

    public TupleIterator getTupleIter(TupleDesc desc){
        LinkedList<Tuple> tuples = new LinkedList<>();

        if (gbfield != NO_GROUPING){
            for (Field fieldvalue : field2Value.keySet()) {
                Integer count = field2Value.get(fieldvalue);
                Tuple tup = new Tuple(desc);
                tup.setField(0, fieldvalue);
                tup.setField(1, new IntField(count));
                tuples.add(tup);
            }
        }
        else {
            Tuple tup = new Tuple(desc);
            Integer count = field2Value.get(NO_GROUPING_FIELD);
            tup.setField(0, new IntField(count));
            tuples.add(tup);
        }
        return new TupleIterator(desc, tuples);
    }

}
