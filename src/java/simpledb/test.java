package simpledb;
import java.io.*;

public class test {

    public static void main(String[] argv) {

        // construct a len-column table schema
        int length = Integer.parseInt(argv[0]);
        Type[] types = new Type[length];
        String[] names = new String[length];

        for (int i=0; i<length; ++i){
            types[i] = Type.INT_TYPE;
            names[i] = "field"+i;
        }

        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("some_data_file_"+length+"_"+length+".dat"), descriptor);
        Database.getCatalog().addTable(table1, "test");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());

        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
                System.out.println("==================");
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
    }

}