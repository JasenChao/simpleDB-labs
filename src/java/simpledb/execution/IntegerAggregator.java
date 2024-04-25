package simpledb.execution;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.StringField;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final Field NO_GROUP_FIELD = new StringField("NO_GROUP_FIELD", 20);
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc td;

    private Map<Field, GroupCalResult> groupCalMap;
    private Map<Field, Tuple> resultMap;

    private static class GroupCalResult {
        public static final Integer DEFAULT_COUNT = 0;
        public static final Integer Deactivate_COUNT = -1;
        public static final Integer DEFAULT_RES = 0;
        public static final Integer Deactivate_RES = -1;
        private Integer result;
        private Integer count;

        public GroupCalResult(int result, int count) {
            this.result = result;
            this.count = count;
        }
    }

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupCalMap = new ConcurrentHashMap<>();
        this.resultMap = new ConcurrentHashMap<>();

        if (this.gbfield == NO_GROUPING) {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        } else {
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
        Field groupByField = this.gbfield == NO_GROUPING ? NO_GROUP_FIELD : tup.getField(this.gbfield);
        if (!NO_GROUP_FIELD.equals(groupByField) && !groupByField.getType().equals(this.gbfieldtype)) {
            throw new IllegalArgumentException("group by field type not match");
        }
        if (!(tup.getField(this.afield) instanceof IntField)) {
            throw new IllegalArgumentException("aggregate field type not match");
        }

        IntField aggregateField = (IntField) tup.getField(this.afield);
        int curVal = aggregateField.getValue();

        switch (this.what) {
            case MIN:
                this.groupCalMap.put(groupByField, new GroupCalResult(Math.min(groupCalMap.getOrDefault(groupByField, new GroupCalResult(Integer.MAX_VALUE, GroupCalResult.Deactivate_COUNT)).result, curVal), GroupCalResult.Deactivate_COUNT));
                break;
            case MAX:
                this.groupCalMap.put(groupByField, new GroupCalResult(Math.max(groupCalMap.getOrDefault(groupByField, new GroupCalResult(Integer.MIN_VALUE, GroupCalResult.Deactivate_COUNT)).result, curVal), GroupCalResult.Deactivate_COUNT));
                break;
            case SUM:
                this.groupCalMap.put(groupByField, new GroupCalResult(groupCalMap.getOrDefault(groupByField, new GroupCalResult(GroupCalResult.DEFAULT_RES, GroupCalResult.Deactivate_COUNT)).result + curVal, GroupCalResult.Deactivate_COUNT));
                break;
            case COUNT:
                this.groupCalMap.put(groupByField, new GroupCalResult(GroupCalResult.Deactivate_RES, groupCalMap.getOrDefault(groupByField, new GroupCalResult(GroupCalResult.Deactivate_RES, GroupCalResult.DEFAULT_COUNT)).count + 1));
                break;
            case AVG:
                GroupCalResult groupCalResult = groupCalMap.getOrDefault(groupByField, new GroupCalResult(GroupCalResult.DEFAULT_RES, GroupCalResult.DEFAULT_COUNT));
                this.groupCalMap.put(groupByField, new GroupCalResult(groupCalResult.result + curVal, groupCalResult.count + 1));
                break;
            case SUM_COUNT:
                break;
            case SC_AVG:
                break;
        }

        Tuple curCalTuple = new Tuple(this.td);
        int curCalRes = 0;
        if (this.what == Op.MIN || this.what == Op.MAX || this.what == Op.SUM) {
            curCalRes = groupCalMap.get(groupByField).result;
        } else if (this.what == Op.COUNT) {
            curCalRes = groupCalMap.get(groupByField).count;
        } else if (this.what == Op.AVG) {
            curCalRes = groupCalMap.get(groupByField).result / groupCalMap.get(groupByField).count;
        }
        if (this.gbfield == NO_GROUPING) {
            curCalTuple.setField(0, new IntField(curCalRes));
        } else {
            curCalTuple.setField(0, groupByField);
            curCalTuple.setField(1, new IntField(curCalRes));
        }

        this.resultMap.put(groupByField, curCalTuple);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here
        return new IntAggTupIterator();
    }

    private class IntAggTupIterator implements OpIterator {
        private boolean isopen = false;
        private Iterator<Map.Entry<Field, Tuple>> iterator;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.iterator = resultMap.entrySet().iterator();
            this.isopen = true;
        }

        @Override
        public void close() {
            this.isopen = false;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (!this.isopen) {
                throw new DbException("Iterator is not open");
            }
            close();
            open();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!this.isopen) {
                throw new DbException("Iterator is not open");
            }
            return this.iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!this.isopen) {
                throw new DbException("Iterator is not open");
            }
            if (!hasNext()) {
                throw new NoSuchElementException("No more tuples");
            }
            Map.Entry<Field, Tuple> entry = this.iterator.next();
            return entry.getValue();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return td;
        }
    }
}
