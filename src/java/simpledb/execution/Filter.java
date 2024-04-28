package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate p;
    private TupleDesc td;
    private OpIterator[] child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // TODO: some code goes here
        this.p = p;
        this.child = new OpIterator[1];
        this.child[0] = child;
        this.td = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        // TODO: some code goes here
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return this.td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // TODO: some code goes here
        super.open();
        this.child[0].open();
    }

    public void close() {
        // TODO: some code goes here
        super.close();
        this.child[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        this.child[0].rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // TODO: some code goes here
        while (this.child[0].hasNext()) {
            Tuple t = this.child[0].next();
            if (this.p.filter(t)) {
                return t;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return this.child;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // TODO: some code goes here
        this.child = children;
    }

}
