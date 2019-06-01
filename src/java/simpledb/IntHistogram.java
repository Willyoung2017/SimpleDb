package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private int[] histogram;
    private double interval;
    private int ntuples;
    private int width;
    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.ntuples = 0;
        this.histogram = new int[buckets];
        this.interval = ((double)(this.max - this.min + 1)) / ((double)this.buckets);
//        System.out.println("int "+Math.ceil(this.interval));
        this.width = (int) (Math.ceil(this.interval));
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here

        int order = Math.min(((v - this.min) / this.width), this.buckets - 1);
//        System.out.println("=======");
//        System.out.println(v);
//        System.out.println(this.min);
//        System.out.println(this.max);
//
//        System.out.println(this.interval);
//        System.out.println(order);
//        System.out.println(this.buckets);
//        System.out.println("=======");

        this.histogram[order] += 1;
        this.ntuples += 1;

    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        double selectivity;
//        for (int i = 0; i < this.buckets; ++i){
//            System.out.println(this.histogram[i]);
//        }
        if (v < this.min || v > this.max) {
            boolean flag = false;
            if (v < this.min)
                flag = true;

            switch(op) {
                case GREATER_THAN_OR_EQ:
                case GREATER_THAN:
                    if (flag)
                        selectivity = 1.0;
                    else
                        selectivity = 0.0;
                    break;
                case NOT_EQUALS:
                    selectivity = 1.0;
                    break;
                case EQUALS:
                    selectivity = 0.0;
                    break;
                case LESS_THAN_OR_EQ:
                case LESS_THAN:
                    if (flag)
                        selectivity = 0.0;
                    else
                        selectivity = 1.0;
                    break;
                default:
                    return 0.0;
//                    throw new RuntimeException("Unhandled op!");
            }
            return selectivity;
        }

        int b = Math.min(((v - this.min) / this.width), this.buckets - 1);


        double b_right = this.min + (b + 1) * this.width;
        double b_left = this.min + b * this.width;
        int h = this.histogram[b];
//        System.out.println("v "+v);
//
//        System.out.println("h "+h);
//        System.out.println("interval "+this.interval);
//        System.out.println("ntuples "+this.ntuples);

        int tmp = 0;
        switch(op){
            case LESS_THAN_OR_EQ:
                for (int i = 0; i < b; i++) {
                    tmp += this.histogram[i];
                }
//                System.out.println(b_left);
                selectivity = ((v - b_left + 1) / (double) this.width * h + tmp) / this.ntuples;
//                selectivity =  estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
                break;

            case GREATER_THAN:
                for (int i = b+1; i < this.buckets; i++) {
                    tmp += this.histogram[i];
                }
                selectivity = ((b_right - v) / (double) this.width * h + tmp) / this.ntuples;
//                if (op == Predicate.Op.LESS_THAN_OR_EQ)
//                    selectivity = 1 - selectivity;
                break;

            case NOT_EQUALS:
            case EQUALS:
                selectivity = ((double)h / (double) this.width) / this.ntuples;
//                System.out.println("sele "+((double)h / (double) this.width));
                if (op == Predicate.Op.NOT_EQUALS)
                    selectivity = 1 - selectivity;
                break;

            case GREATER_THAN_OR_EQ:
                for (int i = b+1; i < this.buckets; i++) {
                    tmp += this.histogram[i];
                }
                selectivity = ((b_right - v + 1) / (double) this.width * h + tmp) / this.ntuples;
//                selectivity = estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
                break;

            case LESS_THAN:
                for (int i = 0; i < b; i++) {
//                    System.out.println(i+" "+this.histogram[i]);
                    tmp += this.histogram[i];
                }
//                System.out.println(b_left);

                selectivity = ((v - b_left) / (double) this.width * h + tmp) / this.ntuples;
                break;
            case LIKE:
                return 1.0;
            default:
                return 0.0;

        }
//        System.out.println(selectivity);
        selectivity = Math.min(selectivity, 1.0);

        return selectivity;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
