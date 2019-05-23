package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc tupleDesc;
    private RecordId redId;
    private Field[] fieldValues;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.tupleDesc = td;
        this.redId = null;
        this.fieldValues = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return redId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.redId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (i >= this.fieldValues.length || i < 0){
            throw new IllegalArgumentException("Index must be a valid index.");
        }
        this.fieldValues[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if (i >= this.fieldValues.length || i < 0){
            throw new IllegalArgumentException("Index must be a valid index.");
        }
        return this.fieldValues[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuilder output = new StringBuilder();
        for (int i=0; i<this.fieldValues.length; ++i) {
            Field t = this.fieldValues[i];
            if (i == (this.fieldValues.length - 1)) {
                output.append(t.toString()).append("\n");
            } else {
                output.append(t.toString()).append(" ");
            }
        }
        return output.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return new FieldIterator();
    }

    public class FieldIterator implements Iterator<Field>{
        private int curPos;

        @Override
        public Field next() {
            if (hasNext()){
                return (fieldValues[curPos++]);
            }
            else{
                throw new NoSuchElementException("Has no next element!");
            }
        }

        @Override
        public boolean hasNext() {
            return (curPos < fieldValues.length);
        }

        private FieldIterator(){
            this.curPos = 0;
        }
    }
    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        this.tupleDesc = td;
    }

    public static Tuple joinTuples(Tuple tuple1, Tuple tuple2, TupleDesc tupleDesc){
        Tuple merged_tuple = new Tuple(tupleDesc);
        for (int i=0; i < tuple1.getTupleDesc().numFields(); ++i) {
            merged_tuple.setField(i, tuple1.getField(i));
        }
        for (int i=0; i < tuple2.getTupleDesc().numFields(); ++i) {
            merged_tuple.setField(tuple1.getTupleDesc().numFields()+i, tuple2.getField(i));
        }

        return merged_tuple;
    }
}
