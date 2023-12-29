/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.h2.api.ErrorCode;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.index.SingleRowCursor;
import org.h2.message.DbException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStoreException;
import org.h2.mvstore.tx.Transaction;
import org.h2.mvstore.tx.TransactionMap;
import org.h2.mvstore.type.LongDataType;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueNull;
import org.h2.value.VersionedValue;

/**
 * A table stored in a MVStore.
 */
public class MVPrimaryIndex extends MVIndex<Long, SearchRow> {

    private final MVTable mvTable;
    private final String mvMapNameInTransactionMap;
    private final TransactionMap<Long, SearchRow> transactionMap;
    private final AtomicLong lastKey = new AtomicLong();
    public int mainIndexColumn = SearchRow.ROWID_INDEX;

    public MVPrimaryIndex(Database database,
                          MVTable mvTable,
                          int id,
                          IndexColumn[] indexColumns,
                          IndexType indexType) {
        super(mvTable, id, mvTable.name + "_DATA", indexColumns, 0, indexType);

        this.mvTable = mvTable;
        mvMapNameInTransactionMap = "table." + getId();

        RowDataType valueType = mvTable.getRowFactory().getRowDataType();

        Transaction transaction = this.mvTable.transactionStore.begin();

        // 生成mvMap 然后拿 mvMap register到 transaction 以 transactionMap形式返回
        transactionMap = transaction.openMap(mvMapNameInTransactionMap, LongDataType.INSTANCE, valueType);
        transactionMap.mvMap.isVolatile = (!mvTable.isPersistData() || !indexType.isPersistent());

        if (!database.isStarting()) {
            transactionMap.clear();
        }

        // a
        transaction.commit();

        Long k = transactionMap.mvMap.lastKey();    // include uncommitted keys as well

        // 后边的insert的row的起始的key都是以它为起点
        lastKey.set(k == null ? 0 : k);
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public String getPlanSQL() {
        return table.getSQL(new StringBuilder(), TRACE_SQL_FLAGS).append(".tableScan").toString();
    }

    public void setMainIndexColumn(int mainIndexColumn) {
        this.mainIndexColumn = mainIndexColumn;
    }

    public int getMainIndexColumn() {
        return mainIndexColumn;
    }

    @Override
    public void close(SessionLocal session) {
        // ok
    }

    @Override
    public void add(SessionLocal sessionLocal, Row row) {
        if (mainIndexColumn == SearchRow.ROWID_INDEX) {
            if (row.key == 0) {
                row.key = lastKey.incrementAndGet();
            }
        } else {
            row.key = row.getValue(mainIndexColumn).getLong();
        }

        if (mvTable.containsLargeObject) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                if (v instanceof ValueLob) {
                    ValueLob lob = ((ValueLob) v).copy(database, getId());
                    sessionLocal.removeAtCommitStop(lob);
                    if (v != lob) {
                        row.setValue(i, lob);
                    }
                }
            }
        }

        TransactionMap<Long, SearchRow> transactionMap = getTransactionMap(sessionLocal);

        long rowKey = row.key;

        try {
            Row oldRow = (Row) transactionMap.putIfAbsent(rowKey, row);

            if (oldRow != null) {
                int errorCode = ErrorCode.CONCURRENT_UPDATE_1;
                if (transactionMap.getImmediate(rowKey) != null || transactionMap.getFromSnapshot(rowKey) != null) {  // committed
                    errorCode = ErrorCode.DUPLICATE_KEY_1;
                }
                DbException e = DbException.get(errorCode, getDuplicatePrimaryKeyMessage(mainIndexColumn).append(' ').append(oldRow).toString());
                e.setSource(this);
                throw e;
            }
        } catch (MVStoreException e) {
            throw mvTable.convertException(e);
        }

        // because it's possible to directly update the key using the _rowid_ syntax
        long last;
        while (rowKey > (last = lastKey.get())) {
            if (lastKey.compareAndSet(last, rowKey)) {
                break;
            }
        }
    }

    @Override
    public void remove(SessionLocal session, Row row) {
        if (mvTable.getContainsLargeObject()) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                if (v instanceof ValueLob) {
                    session.removeAtCommit((ValueLob) v);
                }
            }
        }
        TransactionMap<Long, SearchRow> map = getTransactionMap(session);
        try {
            Row existing = (Row) map.remove(row.getKey());
            if (existing == null) {
                StringBuilder builder = new StringBuilder();
                getSQL(builder, TRACE_SQL_FLAGS).append(": ").append(row.getKey());
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, builder.toString());
            }
        } catch (MVStoreException e) {
            throw mvTable.convertException(e);
        }
    }

    @Override
    public void update(SessionLocal sessionLocal, Row oldRow, Row newRow) {
        if (mainIndexColumn != SearchRow.ROWID_INDEX) {
            long c = newRow.getValue(mainIndexColumn).getLong();
            newRow.setKey(c);
        }

        long key = oldRow.getKey();

        assert mainIndexColumn != SearchRow.ROWID_INDEX || key != 0;
        assert key == newRow.getKey() : key + " != " + newRow.getKey();

        if (mvTable.getContainsLargeObject()) {
            for (int i = 0, len = oldRow.getColumnCount(); i < len; i++) {
                Value oldValue = oldRow.getValue(i);
                Value newValue = newRow.getValue(i);

                if (oldValue == newValue) {
                    continue;
                }

                if (oldValue instanceof ValueLob) {
                    sessionLocal.removeAtCommit((ValueLob) oldValue);
                }

                if (newValue instanceof ValueLob) {
                    ValueLob lob = ((ValueLob) newValue).copy(database, getId());
                    sessionLocal.removeAtCommitStop(lob);
                    if (newValue != lob) {
                        newRow.setValue(i, lob);
                    }
                }
            }
        }

        TransactionMap<Long, SearchRow> transactionMap = getTransactionMap(sessionLocal);

        try {
            Row existing = (Row) transactionMap.put(key, newRow);
            if (existing == null) {
                StringBuilder builder = new StringBuilder();
                getSQL(builder, TRACE_SQL_FLAGS).append(": ").append(key);
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, builder.toString());
            }
        } catch (MVStoreException e) {
            throw mvTable.convertException(e);
        }

        // because it's possible to directly update the key using the _rowid_ syntax
        if (newRow.getKey() > lastKey.get()) {
            lastKey.set(newRow.getKey());
        }
    }

    /**
     * Lock a single row.
     *
     * @param sessionLocal database session
     * @param row          to lock
     * @return row object if it exists
     */
    Row lockRow(SessionLocal sessionLocal, Row row) {
        TransactionMap<Long, SearchRow> transactionMap = getTransactionMap(sessionLocal);
        return lockRow(transactionMap, row.key);
    }

    private Row lockRow(TransactionMap<Long, SearchRow> transactionMap, long key) {
        try {
            return setRowKey((Row) transactionMap.lock(key), key);
        } catch (MVStoreException ex) {
            throw mvTable.convertException(ex);
        }
    }

    @Override
    public Cursor find(SessionLocal sessionLocal, SearchRow first, SearchRow last) {
        long min = extractPKFromRow(first, Long.MIN_VALUE);
        long max = extractPKFromRow(last, Long.MAX_VALUE);
        return find(sessionLocal, min, max);
    }

    private long extractPKFromRow(SearchRow searchRow, long defaultValue) {

        if (searchRow == null) {
            return defaultValue;
        }

        if (mainIndexColumn == SearchRow.ROWID_INDEX) {
            return searchRow.getKey();
        }

        Value value = searchRow.getValue(mainIndexColumn);
        if (value == null) {
            return searchRow.getKey();
        }

        if (value == ValueNull.INSTANCE) {
            return 0L;
        }

        return value.getLong();
    }


    @Override
    public MVTable getTable() {
        return mvTable;
    }

    @Override
    public Row getRow(SessionLocal session, long key) {
        TransactionMap<Long, SearchRow> map = getTransactionMap(session);
        Row row = (Row) map.getFromSnapshot(key);
        if (row == null) {
            throw DbException.get(ErrorCode.ROW_NOT_FOUND_IN_PRIMARY_INDEX, getTraceSQL(), String.valueOf(key));
        }
        return setRowKey(row, key);
    }

    @Override
    public double getCost(SessionLocal sessionLocal,
                          int[] masks,
                          TableFilter[] filters,
                          int filter,
                          SortOrder sortOrder,
                          AllColumnsForPlan allColumnsSet) {
        try {
            return 10 * getCostRangeIndex(masks, transactionMap.sizeAsLongMax(), filters, filter, sortOrder, true, allColumnsSet);
        } catch (MVStoreException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public int getColumnIndex(Column col) {
        // can not use this index - use the delegate index instead
        return SearchRow.ROWID_INDEX;
    }

    @Override
    public boolean isFirstColumn(Column column) {
        return false;
    }

    @Override
    public void remove(SessionLocal session) {
        TransactionMap<Long, SearchRow> map = getTransactionMap(session);
        if (!map.isClosed()) {
            Transaction t = session.getTransaction();
            t.removeMap(map);
        }
    }

    @Override
    public void truncate(SessionLocal session) {
        if (mvTable.getContainsLargeObject()) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
        getTransactionMap(session).clear();
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(SessionLocal session, boolean first) {
        TransactionMap<Long, SearchRow> map = getTransactionMap(session);
        Entry<Long, SearchRow> entry = first ? map.firstEntry() : map.lastEntry();
        return new SingleRowCursor(entry != null ? setRowKey((Row) entry.getValue(), entry.getKey()) : null);
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public long getRowCount(SessionLocal sessionLocal) {
        return getTransactionMap(sessionLocal).sizeAsLong();
    }

    /**
     * The maximum number of rows, including uncommitted rows of any session.
     *
     * @return the maximum number of rows
     */
    public long getRowCountMax() {
        return transactionMap.sizeAsLongMax();
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return getRowCountMax();
    }

    @Override
    public long getDiskSpaceUsed() {
        return transactionMap.mvMap.getRootPage().getDiskSpaceUsed();
    }

    public String getMvMapNameInTransactionMap() {
        return mvMapNameInTransactionMap;
    }

    @Override
    public void addRowsToTemporaryMvMap(List<Row> rows, String temporaryMvMapName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addBufferedRows(List<String> bufferNames) {
        throw new UnsupportedOperationException();
    }

    private Cursor find(SessionLocal sessionLocal, Long first, Long last) {
        TransactionMap<Long, SearchRow> transactionMap = getTransactionMap(sessionLocal);

        if (first != null && last != null && first.longValue() == last.longValue()) {
            return new SingleRowCursor(setRowKey((Row) transactionMap.getFromSnapshot(first), first));
        }

        return new MVStoreCursor(transactionMap.entryIterator(first, last));
    }

    @Override
    public boolean isRowIdIndex() {
        return true;
    }

    /**
     * Get the map to store the data.
     *
     * @param sessionLocal the session
     * @return the map
     */
    TransactionMap<Long, SearchRow> getTransactionMap(SessionLocal sessionLocal) {
        if (sessionLocal == null) {
            return transactionMap;
        }

        Transaction transaction = sessionLocal.getTransaction();

        // 把 transactionMap 内部的 mvMap register 到 transaction 然后以 transactionMap返回
        return transactionMap.getInstance(transaction);
    }

    @Override
    public MVMap<Long, VersionedValue<SearchRow>> getMVMap() {
        return transactionMap.mvMap;
    }

    private static Row setRowKey(Row row, long key) {
        if (row != null && row.getKey() == 0) {
            row.setKey(key);
        }
        return row;
    }

    static final class MVStoreCursor implements Cursor {

        private final TransactionMap.TransactionMapIterator<Long, SearchRow, Entry<Long, SearchRow>> transactionMapIterator;
        private Entry<Long, SearchRow> currentEntry;
        private Row row;

        public MVStoreCursor(TransactionMap.TransactionMapIterator<Long, SearchRow, Entry<Long, SearchRow>> transactionMapIterator) {
            this.transactionMapIterator = transactionMapIterator;
        }

        @Override
        public Row get() {
            if (row == null) {
                if (currentEntry != null) {
                    row = (Row) currentEntry.getValue();

                    if (row.getKey() == 0) {
                        row.setKey(currentEntry.getKey());
                    }
                }
            }

            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return get();
        }

        @Override
        public boolean next() {
            currentEntry = transactionMapIterator.fetchNext();
            row = null;
            return currentEntry != null;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }
    }
}
