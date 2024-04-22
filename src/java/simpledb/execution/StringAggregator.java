package simpledb.execution;

import java.util.Iterator;
import java.util.Map;
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
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final Field NO_GROUP_FIELD = new StringField("NO_GROUP_FIELD", 20);
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private TupleDesc td;

    private Map<Field, Integer> groupCalMap;
    private Map<Field, Tuple> resultMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator only supports COUNT");
        }

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        
        this.groupCalMap = new ConcurrentHashMap<>();
        this.resultMap = new ConcurrentHashMap<>();

        if (this.gbfield == NO_GROUPING) {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        } else {
            this.td = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
        Field groupField = this.gbfield == NO_GROUPING ? NO_GROUP_FIELD : tup.getField(this.gbfield);
        if (!NO_GROUP_FIELD.equals(groupField) && groupField.getType() != this.gbfieldtype) {
            throw new IllegalArgumentException("group field type mismatch");
        }
        if (!(tup.getField(this.afield) instanceof StringField)) {
            throw new IllegalArgumentException("aggregate field type mismatch");
        }

        this.groupCalMap.put(groupField, this.groupCalMap.getOrDefault(groupField, 0) + 1);
        Tuple curCaTuple = new Tuple(td);
        if (this.gbfield == NO_GROUPING) {
            curCaTuple.setField(0, new IntField(this.groupCalMap.get(groupField)));
        } else {
            curCaTuple.setField(0, groupField);
            curCaTuple.setField(1, new IntField(this.groupCalMap.get(groupField)));
        }

        resultMap.put(groupField, curCaTuple);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here
        return new StringAggTupIterator();
    }

    private class StringAggTupIterator implements OpIterator {
        private boolean isOpen = false;
        private Iterator<Map.Entry<Field, Tuple>> it;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            it = resultMap.entrySet().iterator();
            isOpen = true;
        }

        @Override
        public void close() {
            isOpen = false;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                throw new DbException("Iterator is not open");
            }
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                throw new DbException("Iterator is not open");
            }
            return it.next().getValue();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                throw new DbException("Iterator is not open");
            }
            close();
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return td;
        }
    }
}
