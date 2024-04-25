package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] buckets;
    private int min;
    private int max;
    private double width;
    private int ntups;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // TODO: some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.width = Math.max(1, (max - min + 1.0) / buckets);
        this.ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // TODO: some code goes here
        if (v < min || v > max) {
            return;
        }
        buckets[getBucketId(v)]++;
        ntups++;
    }

    public int getBucketId(int v) {
        return (int) ((v - min) / width);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        // TODO: some code goes here
        switch (op) {
            case EQUALS:
                return estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v) - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            case GREATER_THAN:
                if (v > max) {
                    return 0.0;
                } else if (v <= min) {
                    return 1.0;
                } else {
                    int bucketId = getBucketId(v);
                    double tuples = 0.0;
                    for (int i = bucketId + 1; i < buckets.length; i++) {
                        tuples += buckets[i];
                    }
                    tuples += (min + (bucketId + 1) * width - v - 1) * (1.0 * buckets[bucketId] / width);
                    return tuples / ntups;
                }
            case LESS_THAN:
                return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v - 1);
            case LESS_THAN_OR_EQ:
                return 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
            case NOT_EQUALS:
                return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
            default:
                throw new IllegalArgumentException("Unsupported operator " + op);
        }
    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // TODO: some code goes here
        double sum = 0.0;
        for (int bucket : buckets) {
            sum += 1.0 * bucket / ntups;
        }
        return sum / buckets.length;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // TODO: some code goes here
        return "IntHistogram{" +
                "buckets=" + java.util.Arrays.toString(buckets) +
                ", min=" + min +
                ", max=" + max +
                ", width=" + width +
                ", ntups=" + ntups +
                '}';
    }
}
