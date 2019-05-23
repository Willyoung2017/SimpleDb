package simpledb;

import java.io.Serializable;
import java.util.*;


/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /* all the field TDItems included */
    private List<TDItem> tdAr;
    private int numFields;
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here

        return new TDItemIterator();
    }

    public class TDItemIterator implements Iterator<TDItem>{
        private int curPos;

        @Override
        public TDItem next() {
            if (hasNext()){
                return (tdAr.get(curPos++));
            }
            else{
                throw new NoSuchElementException("Has no next element!");
            }
        }

        @Override
        public boolean hasNext() {
            return (curPos < tdAr.size());
        }

        private TDItemIterator(){
            this.curPos = 0;
        }
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr.length == 0){
            throw new IllegalArgumentException("typeAr must contain at least one entry!");
        }
        else if (typeAr.length != fieldAr.length){
            throw new IllegalArgumentException("typeAr and fieldAr must be of same length!");
        }

        int idx = 0;
        List<TDItem> tdAr = new LinkedList<>();
        for (Type t: typeAr){
            String f = fieldAr[idx];
            idx += 1;
            TDItem tdi = new TDItem(t, f);
            tdAr.add(tdi);
        }
        this.tdAr = tdAr;
        this.numFields = typeAr.length;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        if (typeAr.length == 0) {
            throw new IllegalArgumentException("typeAr must contain at least one entry!");
        }

        List<TDItem> tdAr = new LinkedList<>();
        for (Type t: typeAr){
            TDItem tdi = new TDItem(t,null);
            tdAr.add(tdi);
        }
        this.tdAr = tdAr;
        this.numFields = typeAr.length;
    }

    public TupleDesc(List<TDItem> tdAr) {
        this.tdAr = tdAr;
        this.numFields = tdAr.size();
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= this.numFields || i < 0){
            throw new NoSuchElementException("i is not a valid field reference!");
        }

        return this.tdAr.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= this.numFields || i < 0){
            throw new NoSuchElementException("i is not a valid field reference!");
        }

        return this.tdAr.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null){
            throw new NoSuchElementException("Name to search cannot be null!");
        }
//        System.out.println("Start...");
//        System.out.println("======================");
//        for (TDItem td : this.tdAr){
//           System.out.println(name +"..."+td.fieldName);
//        }
//        System.out.println("======================");

        int idx = 0;
        for (int i = 0; i < this.numFields; ++i){
            TDItem td = this.tdAr.get(i);
//            System.out.println(idx);
            if ((td.fieldName != null) && (td.fieldName.equals(name))){
//                System.out.println("out");
                return idx;
            }
            idx += 1;
        }
        throw new NoSuchElementException("No field with a matching name is found!");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int tupleSize = 0;
        for (TDItem td : tdAr){
            tupleSize += td.fieldType.getLen();
        }
        return tupleSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here

        List<TDItem> tdAr = new LinkedList<>();

        tdAr.addAll(td1.tdAr);
        tdAr.addAll(td2.tdAr);


        return new TupleDesc(tdAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (this == o){
            return true;
        }
        if (o instanceof TupleDesc){
            if (((TupleDesc) o).numFields()==this.numFields()){
                for (int i=0; i<this.numFields; ++i){
                    if (!((TupleDesc) o).tdAr.get(i).fieldType.equals(this.tdAr.get(i).fieldType)){
                        return false;
                    }
                }
                return true;
            }
            return false;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder output = new StringBuilder();
        output.append("[ ");
        for (int i=0; i<tdAr.size(); ++i) {
            TDItem td = tdAr.get(i);
            if (i == (tdAr.size() - 1)) {
                output.append(td.toString()).append(" ]");
            } else {
                output.append(td.toString()).append(", ");
            }
        }
        return output.toString();
    }
}
