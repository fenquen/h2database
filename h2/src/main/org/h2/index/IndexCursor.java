/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.ArrayList;

import org.h2.engine.SessionLocal;
import org.h2.expression.condition.Comparison;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.value.Value;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueNull;

/**
 * The filter used to walk through an index. This class supports IN(..)
 * and IN(SELECT ...) optimizations.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class IndexCursor implements Cursor {

    private SessionLocal sessionLocal;
    private Index index; // prepare plan 的 时候 set mvPrimaryIndex
    private Table table;
    private IndexColumn[] indexColumns;
    private boolean alwaysFalse;

    private SearchRow start, end, intersects;
    public Cursor cursor;
    private Column inColumn;
    private int inListIndex;
    private Value[] inList;
    private ResultInterface inResult;

    public IndexCursor() {
    }

    public void setIndex(Index index) {
        this.index = index;
        this.table = index.getTable();
        Column[] columns = table.getColumns();
        indexColumns = new IndexColumn[columns.length];
        IndexColumn[] idxCols = index.getIndexColumns();
        if (idxCols != null) {
            for (int i = 0, len = columns.length; i < len; i++) {
                int idx = index.getColumnIndex(columns[i]);
                if (idx >= 0) {
                    indexColumns[i] = idxCols[idx];
                }
            }
        }
    }

    /**
     * Prepare this index cursor to make a lookup in index.
     *
     * @param sessionLocal Session.
     * @param indexConditions Index conditions.
     */
    public void prepare(SessionLocal sessionLocal, ArrayList<IndexCondition> indexConditions) {
        this.sessionLocal = sessionLocal;

        alwaysFalse = false;

        start  = null;
        end = null;

        inList = null;
        inColumn = null;
        inResult = null;
        intersects = null;

        for (IndexCondition indexCondition : indexConditions) {
            if (indexCondition.isAlwaysFalse()) {
                alwaysFalse = true;
                break;
            }

            // If index can perform only full table scan do not try to use it for regular
            // lookups, each such lookup will perform an own table scan.
            if (index.isFindUsingFullTableScan()) {
                continue;
            }

            Column column = indexCondition.column;

            if (indexCondition.compareType == Comparison.IN_LIST) {
                if (start == null && end == null) {
                    if (canUseIndexForIn(column)) {
                        this.inColumn = column;
                        inList = indexCondition.getCurrentValueList(sessionLocal);
                        inListIndex = 0;
                    }
                }
            } else if (indexCondition.compareType == Comparison.IN_QUERY) {
                if (start == null && end == null) {
                    if (canUseIndexForIn(column)) {
                        this.inColumn = column;
                        inResult = indexCondition.getCurrentResult();
                    }
                }
            } else {
                Value v = indexCondition.getCurrentValue(sessionLocal);

                boolean isStart = indexCondition.isStart();
                boolean isEnd = indexCondition.isEnd();
                boolean isIntersects = indexCondition.isSpatialIntersects();

                if (column.id != SearchRow.ROWID_INDEX) {
                    IndexColumn indexColumn = indexColumns[column.id];

                    // if the index column is sorted the other way, we swap end and start NULLS_FIRST / NULLS_LAST is not a problem
                    // as nulls never match anyway
                    if (indexColumn != null && (indexColumn.sortType & SortOrder.DESCENDING) != 0) {
                        boolean temp = isStart;
                        isStart = isEnd;
                        isEnd = temp;
                    }
                }

                if (isStart) {
                    start = getSearchRow(start, column.id, v, true);
                }

                if (isEnd) {
                    end = getSearchRow(end, column.id, v, false);
                }

                if (isIntersects) {
                    intersects = getSpatialSearchRow(intersects, column.id, v);
                }

                // a X=? condition will produce less rows than an X IN(..) condition, unless the X IN condition can use the index.
                if ((isStart || isEnd) && !canUseIndexFor(inColumn)) {
                    inColumn = null;
                    inList = null;
                    inResult = null;
                }
            }
        }

        if (inColumn != null) {
            start = table.getTemplateRow();
        }
    }

    /**
     * Re-evaluate the start and end values of the index search for rows.
     *
     * @param sessionLocal the session
     * @param indexConditions the index conditions
     */
    public void find(SessionLocal sessionLocal, ArrayList<IndexCondition> indexConditions) {
        prepare(sessionLocal, indexConditions);

        if (inColumn != null) {
            return;
        }

        if (alwaysFalse) {
            return;
        }

        if (intersects != null && index instanceof SpatialIndex) {
            cursor = ((SpatialIndex) index).findByGeometry(this.sessionLocal, start, end, intersects);
        } else if (index != null) {
            cursor = index.find(this.sessionLocal, start, end);
        }
    }

    private boolean canUseIndexForIn(Column column) {
        if (inColumn != null) {
            // only one IN(..) condition can be used at the same time
            return false;
        }
        return canUseIndexFor(column);
    }

    private boolean canUseIndexFor(Column column) {
        // The first column of the index must match this column,
        // or it must be a VIEW index (where the column is null).
        // Multiple IN conditions with views are not supported, see
        // IndexCondition.getMask.
        IndexColumn[] cols = index.getIndexColumns();
        if (cols == null) {
            return true;
        }
        IndexColumn idxCol = cols[0];
        return idxCol == null || idxCol.column == column;
    }

    private SearchRow getSpatialSearchRow(SearchRow row, int columnId, Value v) {
        if (row == null) {
            row = table.getTemplateRow();
        } else if (row.getValue(columnId) != null) {
            // if an object needs to overlap with both a and b,
            // then it needs to overlap with the union of a and b
            // (not the intersection)
            ValueGeometry vg = row.getValue(columnId).convertToGeometry(null);
            v = v.convertToGeometry(null).getEnvelopeUnion(vg);
        }
        if (columnId == SearchRow.ROWID_INDEX) {
            row.setKey(v == ValueNull.INSTANCE ? Long.MIN_VALUE : v.getLong());
        } else {
            row.setValue(columnId, v);
        }
        return row;
    }

    private SearchRow getSearchRow(SearchRow searchRow,
                                   int columnId,
                                   Value value,
                                   boolean max) {
        if (searchRow == null) {
            searchRow = table.getTemplateRow();
        } else {
            value = getMax(searchRow.getValue(columnId), value, max);
        }

        if (columnId == SearchRow.ROWID_INDEX) {
            searchRow.setKey(value == ValueNull.INSTANCE ? Long.MIN_VALUE : value.getLong());
        } else {
            searchRow.setValue(columnId, value);
        }

        return searchRow;
    }

    private Value getMax(Value a, Value b, boolean bigger) {
        if (a == null) {
            return b;
        }

        if (b == null) {
            return a;
        }

        // IS NULL must be checked later
        if (a == ValueNull.INSTANCE) {
            return b;
        }

        if (b == ValueNull.INSTANCE) {
            return a;
        }

        int comp = sessionLocal.compare(a, b);

        if (comp == 0) {
            return a;
        }

        return (comp > 0) == bigger ? a : b;
    }

    /**
     * Check if the result is empty for sure.
     *
     * @return true if it is
     */
    public boolean isAlwaysFalse() {
        return alwaysFalse;
    }

    /**
     * Get start search row.
     *
     * @return search row
     */
    public SearchRow getStart() {
        return start;
    }

    /**
     * Get end search row.
     *
     * @return search row
     */
    public SearchRow getEnd() {
        return end;
    }

    @Override
    public Row getCurrentRow() {
        if (cursor == null) {
            return null;
        }

        return cursor.getCurrentRow();
    }

    @Override
    public SearchRow getCurrentSearchRow() {
        return cursor.getCurrentSearchRow();
    }

    @Override
    public boolean next() {
        while (true) {
            if (cursor == null) {
                nextCursor();

                if (cursor == null) {
                    return false;
                }
            }

            if (cursor.next()) {
                return true;
            }

            cursor = null;
        }
    }

    private void nextCursor() {
        if (inList != null) {
            while (inListIndex < inList.length) {
                Value v = inList[inListIndex++];
                if (v != ValueNull.INSTANCE) {
                    find(v);
                    break;
                }
            }
        } else if (inResult != null) {
            while (inResult.next()) {
                Value v = inResult.currentRow()[0];
                if (v != ValueNull.INSTANCE) {
                    find(v);
                    break;
                }
            }
        }
    }

    private void find(Value v) {
        v = inColumn.convert(sessionLocal, v);
        int id = inColumn.getId();
        start.setValue(id, v);
        cursor = index.find(sessionLocal, start, start);
    }

    @Override
    public boolean previous() {
        throw DbException.getInternalError(toString());
    }

}
