/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;

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
import org.h2.mvstore.tx.TransactionMap.TransactionMapIterator;
import org.h2.mvstore.type.DataType;
import org.h2.result.Row;
import org.h2.result.RowFactory;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.VersionedValue;

public final class MVSecondaryIndex extends MVIndex<SearchRow, Value> {

    private final MVTable mvTable;

    /**
     * value 是ValueNull.INSTANCE
     */
    private final TransactionMap<SearchRow, Value> transactionMap;

    public MVSecondaryIndex(Database database,
                            MVTable mvTable,
                            int id,
                            String indexName,
                            IndexColumn[] columns,
                            int uniqueColumnCount,
                            IndexType indexType) {
        super(mvTable, id, indexName, columns, uniqueColumnCount, indexType);

        if (!this.database.isStarting()) {
            checkIndexColumnTypes(columns);
        }

        String mapName = "index." + getId();
        RowDataType keyType = getRowFactory().getRowDataType();

        this.mvTable = mvTable;

        Transaction transaction = this.mvTable.getTransactionBegin();

        transactionMap = transaction.openMap(mapName, keyType, NullValueDataType.INSTANCE);
        transactionMap.mvMap.setVolatile(!mvTable.isPersistData() || !indexType.isPersistent());

        if (!database.isStarting()) {
            transactionMap.clear();
        }

        transaction.commit();

        if (!keyType.equals(transactionMap.getKeyType())) {
            throw DbException.getInternalError("incompatible key type, expected " + keyType + " but got " + transactionMap.getKeyType() + " for index " + indexName);
        }
    }

    @Override
    public void addRowsToTemporaryMvMap(List<Row> rows, String temporaryMvMapName) {
        MVMap<SearchRow, Value> map = openMap(temporaryMvMapName);
        for (Row row : rows) {
            SearchRow r = getRowFactory().createRow();
            r.copyFrom(row);
            map.append(r, ValueNull.INSTANCE);
        }
    }

    private static final class Source {

        private final Iterator<SearchRow> iterator;

        SearchRow currentSearchRow;

        public Source(Iterator<SearchRow> iterator) {
            assert iterator.hasNext();
            this.iterator = iterator;
            this.currentSearchRow = iterator.next();
        }

        public boolean hasNext() {
            boolean result = iterator.hasNext();
            if (result) {
                currentSearchRow = iterator.next();
            }
            return result;
        }

        public SearchRow next() {
            return currentSearchRow;
        }

        static final class Comparator implements java.util.Comparator<Source> {

            private final DataType<SearchRow> dataType;

            public Comparator(DataType<SearchRow> dataType) {
                this.dataType = dataType;
            }

            @Override
            public int compare(Source one, Source two) {
                return dataType.compare(one.currentSearchRow, two.currentSearchRow);
            }
        }
    }

    @Override
    public void addBufferedRows(List<String> temporaryMvMapNameList) {
        Queue<Source> priorityQueue = new PriorityQueue<>(temporaryMvMapNameList.size(), new Source.Comparator(getRowFactory().getRowDataType()));

        for (String temporaryMvMapName : temporaryMvMapNameList) {
            Iterator<SearchRow> iterator = openMap(temporaryMvMapName).keyIterator(null);
            if (iterator.hasNext()) {
                priorityQueue.offer(new Source(iterator));
            }
        }

        try {
            while (!priorityQueue.isEmpty()) {
                Source source = priorityQueue.poll();
                SearchRow searchRow = source.next();

                if (uniqueColumnColumn > 0 && !mayHaveNullDuplicates(searchRow)) {
                    checkUnique(false, transactionMap, searchRow, Long.MIN_VALUE);
                }

                transactionMap.putCommitted(searchRow, ValueNull.INSTANCE);

                if (source.hasNext()) {
                    priorityQueue.offer(source);
                }
            }
        } finally {
            for (String tempMapName : temporaryMvMapNameList) {
                database.store.mvStore.removeMap(tempMapName);
            }
        }
    }

    private MVMap<SearchRow, Value> openMap(String mapName) {
        RowDataType keyType = getRowFactory().getRowDataType();
        MVMap.Builder<SearchRow, Value> builder = new MVMap.Builder<SearchRow, Value>().singleWriter().keyType(keyType).valueType(NullValueDataType.INSTANCE);
        MVMap<SearchRow, Value> map = database.getStore().getMvStore().openMap(mapName, builder);
        if (!keyType.equals(map.getKeyType())) {
            throw DbException.getInternalError("incompatible key type, expected " + keyType + " but got " + map.getKeyType() + " for map " + mapName);
        }
        return map;
    }

    @Override
    public void close(SessionLocal session) {
        // ok
    }

    @Override
    public void add(SessionLocal sessionLocal, Row row) {
        TransactionMap<SearchRow, Value> map = getMap(sessionLocal);
        SearchRow key = convertToKey(row, null);
        boolean checkRequired = uniqueColumnColumn > 0 && !mayHaveNullDuplicates(row);
        if (checkRequired) {
            boolean repeatableRead = !sessionLocal.getTransaction().allowNonRepeatableRead();
            checkUnique(repeatableRead, map, row, Long.MIN_VALUE);
        }

        try {
            map.put(key, ValueNull.INSTANCE);
        } catch (MVStoreException e) {
            throw mvTable.convertException(e);
        }

        if (checkRequired) {
            checkUnique(false, map, row, row.getKey());
        }
    }

    private void checkUnique(boolean repeatableRead, TransactionMap<SearchRow, Value> map, SearchRow row, long newKey) {
        RowFactory uniqueRowFactory = getUniqueRowFactory();
        SearchRow from = uniqueRowFactory.createRow();
        from.copyFrom(row);
        from.setKey(Long.MIN_VALUE);
        SearchRow to = uniqueRowFactory.createRow();
        to.copyFrom(row);
        to.setKey(Long.MAX_VALUE);
        if (repeatableRead) {
            // In order to guarantee repeatable reads, snapshot taken at the beginning of the statement or transaction
            // need to be checked additionally, because existence of the key should be accounted for,
            // even if since then, it was already deleted by another (possibly committed) transaction.
            TransactionMapIterator<SearchRow, Value, SearchRow> it = map.keyIterator(from, to);
            for (SearchRow k; (k = it.fetchNext()) != null; ) {
                if (newKey != k.getKey() && !map.isDeletedByCurrentTransaction(k)) {
                    throw getDuplicateKeyException(k.toString());
                }
            }
        }
        TransactionMapIterator<SearchRow, Value, SearchRow> it = map.keyIteratorUncommitted(from, to);
        for (SearchRow k; (k = it.fetchNext()) != null; ) {
            if (newKey != k.getKey()) {
                if (map.getImmediate(k) != null) {
                    // committed
                    throw getDuplicateKeyException(k.toString());
                }
                throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, table.getName());
            }
        }
    }

    @Override
    public void remove(SessionLocal session, Row row) {
        SearchRow searchRow = convertToKey(row, null);
        TransactionMap<SearchRow, Value> map = getMap(session);
        try {
            if (map.remove(searchRow) == null) {
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
        SearchRow searchRowOld = convertToKey(oldRow, null);
        SearchRow searchRowNew = convertToKey(newRow, null);
        if (!rowsAreEqual(searchRowOld, searchRowNew)) {
            super.update(sessionLocal, oldRow, newRow);
        }
    }

    private boolean rowsAreEqual(SearchRow rowOne, SearchRow rowTwo) {
        if (rowOne == rowTwo) {
            return true;
        }
        for (int index : columnIds) {
            Value v1 = rowOne.getValue(index);
            Value v2 = rowTwo.getValue(index);
            if (!Objects.equals(v1, v2)) {
                return false;
            }
        }
        return rowOne.getKey() == rowTwo.getKey();
    }

    @Override
    public Cursor find(SessionLocal sessionLocal, SearchRow first, SearchRow last) {
        return find(sessionLocal, first, false, last);
    }

    private Cursor find(SessionLocal sessionLocal,
                        SearchRow first,
                        boolean bigger,
                        SearchRow last) {
        SearchRow min = convertToKey(first, bigger);
        SearchRow max = convertToKey(last, Boolean.TRUE);
        return new MVStoreCursor(sessionLocal, getMap(sessionLocal).keyIterator(min, max), mvTable);
    }

    private SearchRow convertToKey(SearchRow r, Boolean minMax) {
        if (r == null) {
            return null;
        }

        SearchRow row = getRowFactory().createRow();

        row.copyFrom(r);

        if (minMax != null) {
            row.setKey(minMax ? Long.MAX_VALUE : Long.MIN_VALUE);
        }

        return row;
    }

    @Override
    public MVTable getTable() {
        return mvTable;
    }

    @Override
    public double getCost(SessionLocal session, int[] masks,
                          TableFilter[] filters, int filter, SortOrder sortOrder,
                          AllColumnsForPlan allColumnsSet) {
        try {
            return 10 * getCostRangeIndex(masks, transactionMap.sizeAsLongMax(),
                    filters, filter, sortOrder, false, allColumnsSet);
        } catch (MVStoreException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public void remove(SessionLocal session) {
        TransactionMap<SearchRow, Value> map = getMap(session);
        if (!map.isClosed()) {
            Transaction t = session.getTransaction();
            t.removeMap(map);
        }
    }

    @Override
    public void truncate(SessionLocal session) {
        TransactionMap<SearchRow, Value> map = getMap(session);
        map.clear();
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(SessionLocal session, boolean first) {
        TransactionMapIterator<SearchRow, Value, SearchRow> iter = getMap(session).keyIterator(null, !first);
        for (SearchRow key; (key = iter.fetchNext()) != null; ) {
            if (key.getValue(columnIds[0]) != ValueNull.INSTANCE) {
                return new SingleRowCursor(mvTable.getRow(session, key.getKey()));
            }
        }
        return new SingleRowCursor(null);
    }

    @Override
    public boolean needRebuild() {
        try {
            return transactionMap.sizeAsLongMax() == 0;
        } catch (MVStoreException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getRowCount(SessionLocal sessionLocal) {
        TransactionMap<SearchRow, Value> map = getMap(sessionLocal);
        return map.sizeAsLong();
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        try {
            return transactionMap.sizeAsLongMax();
        } catch (MVStoreException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getDiskSpaceUsed() {
        // TODO estimate disk space usage
        return 0;
    }

    @Override
    public boolean canFindNext() {
        return true;
    }

    @Override
    public Cursor findNext(SessionLocal session, SearchRow higherThan, SearchRow last) {
        return find(session, higherThan, true, last);
    }

    private TransactionMap<SearchRow, Value> getMap(SessionLocal sessionLocal) {
        if (sessionLocal == null) {
            return transactionMap;
        }

        Transaction transaction = sessionLocal.getTransaction();

        return transactionMap.getInstance(transaction);
    }

    @Override
    public MVMap<SearchRow, VersionedValue<Value>> getMVMap() {
        return transactionMap.mvMap;
    }

    /**
     * A cursor.
     */
    static final class MVStoreCursor implements Cursor {

        private final SessionLocal sessionLocal;
        private final TransactionMapIterator<SearchRow, Value, SearchRow> transactionMapIterator;
        private final MVTable mvTable;
        private SearchRow currentSearchRow;
        private Row currentRow;

        MVStoreCursor(SessionLocal sessionLocal,
                      TransactionMapIterator<SearchRow, Value, SearchRow> transactionMapIterator,
                      MVTable mvTable) {
            this.sessionLocal = sessionLocal;
            this.transactionMapIterator = transactionMapIterator;
            this.mvTable = mvTable;
        }

        @Override
        public Row getCurrentRow() {
            if (currentRow == null) {
                if (currentSearchRow != null) {
                    // 体现了索引保存的是row的物理position
                    currentRow = mvTable.getRow(sessionLocal, currentSearchRow.key);
                }
            }

            return currentRow;
        }

        @Override
        public SearchRow getCurrentSearchRow() {
            return currentSearchRow;
        }

        @Override
        public boolean next() {
            currentSearchRow = transactionMapIterator.fetchNext();
            currentRow = null;
            return currentSearchRow != null;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }
    }

}
