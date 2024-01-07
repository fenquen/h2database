/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.query;

import static org.h2.expression.Expression.WITHOUT_PARENTHESES;
import static org.h2.util.HasSQL.ADD_PLAN_INFORMATION;
import static org.h2.util.HasSQL.DEFAULT_SQL_FLAGS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Mode.ExpressionNames;
import org.h2.engine.SessionLocal;
import org.h2.expression.Alias;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionList;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.expression.Wildcard;
import org.h2.expression.analysis.DataAnalysisOperation;
import org.h2.expression.analysis.Window;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionLocalAndGlobal;
import org.h2.expression.function.CoalesceFunction;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.QueryExpressionIndex;
import org.h2.message.DbException;
import org.h2.mode.DefaultNullOrdering;
import org.h2.result.LazyResult;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.result.ResultTarget;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.table.TableType;
import org.h2.table.TableView;
import org.h2.util.ParserUtil;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueRow;

/**
 * This class represents a simple SELECT statement.
 * <p>
 * For each select statement,
 * visibleColumnCount &lt;= distinctColumnCount &lt;= expressionCount.
 * The expression list count could include ORDER BY and GROUP BY expressions
 * that are not in the select list.
 * <p>
 * The call sequence is init(), mapColumns() if it's a subquery, prepare().
 *
 * @author Thomas Mueller
 * @author Joel Turkel (Group sorted query)
 */
public class Select extends Query {

    /**
     * The main (top) table filter.
     */
    TableFilter topTableFilter;

    private final ArrayList<TableFilter> tableFilters = Utils.newSmallArrayList();
    private final ArrayList<TableFilter> topTableFilters = Utils.newSmallArrayList();

    /**
     * Parent select for selects in table filters.
     */
    public final Select parentSelect;

    /**
     * WHERE condition.
     */
    private Expression where;

    /**
     * HAVING condition.
     */
    private Expression having;

    /**
     * QUALIFY condition.
     */
    private Expression qualify;

    /**
     * {@code DISTINCT ON(...)} expressions.
     */
    private Expression[] distinctExpressions;

    private int[] distinctIndexes;

    private ArrayList<Expression> group;

    /**
     * The indexes of the group-by columns.
     */
    int[] groupIndex;

    /**
     * Whether a column in the expression list is part of a group-by.
     */
    boolean[] groupByExpression;

    /**
     * Grouped data for aggregates.
     */
    SelectGroups groupData;

    private int havingIndex;

    private int qualifyIndex;

    private int[] groupByCopies;

    /**
     * Whether this SELECT is an explicit table (TABLE tableName). It is used in
     * {@link #getPlanSQL(int)} to generate SQL similar to original query.
     */
    private boolean isExplicitTable;

    /**
     * This flag is set when SELECT statement contains (non-window) aggregate
     * functions, GROUP BY clause or HAVING clause.
     */
    boolean isGroupQuery;
    private boolean isGroupSortedQuery;
    private boolean isWindowQuery;
    private boolean isForUpdate;
    private double cost;
    private boolean isQuickAggregateQuery, isDistinctQuery;
    private boolean sortUsingIndex;

    private boolean isGroupWindowStage2;

    private HashMap<String, Window> windows;

    public Select(SessionLocal session, Select parentSelect) {
        super(session);
        this.parentSelect = parentSelect;
    }

    @Override
    public boolean isUnion() {
        return false;
    }

    /**
     * Add a table to the query.
     *
     * @param filter the table to add
     * @param isTop  if the table can be the first table in the query plan
     */
    public void addTableFilter(TableFilter filter, boolean isTop) {
        // Oracle doesn't check on duplicate aliases
        // String alias = filter.getAlias();
        // if (filterNames.contains(alias)) {
        //     throw Message.getSQLException(
        //         ErrorCode.DUPLICATE_TABLE_ALIAS, alias);
        // }
        // filterNames.add(alias);
        tableFilters.add(filter);

        if (isTop) {
            topTableFilters.add(filter);
        }
    }

    public ArrayList<TableFilter> getTopTableFilters() {
        return topTableFilters;
    }

    public void setExpressions(ArrayList<Expression> expressions) {
        this.expressionList = expressions;
    }

    /**
     * Convert this SELECT to an explicit table (TABLE tableName).
     */
    public void setExplicitTable() {
        setWildcard();
        isExplicitTable = true;
    }

    /**
     * Sets a wildcard expression as in "SELECT * FROM TEST".
     */
    public void setWildcard() {
        expressionList = new ArrayList<>(1);
        expressionList.add(new Wildcard(null, null));
    }

    /**
     * Set when SELECT statement contains (non-window) aggregate functions,
     * GROUP BY clause or HAVING clause.
     */
    public void setGroupQuery() {
        isGroupQuery = true;
    }

    /**
     * Called if this query contains window functions.
     */
    public void setWindowQuery() {
        isWindowQuery = true;
    }

    public void setGroupBy(ArrayList<Expression> group) {
        this.group = group;
    }

    public ArrayList<Expression> getGroupBy() {
        return group;
    }

    /**
     * Get the group data if there is currently a group-by active.
     *
     * @param window is this a window function
     * @return the grouped data
     */
    public SelectGroups getGroupDataIfCurrent(boolean window) {
        return groupData != null && (window || groupData.isCurrentGroup()) ? groupData : null;
    }

    /**
     * Set the distinct flag.
     */
    public void setDistinct() {
        if (distinctExpressions != null) {
            throw DbException.getUnsupportedException("DISTINCT ON together with DISTINCT");
        }
        distinct = true;
    }

    /**
     * Set the DISTINCT ON expressions.
     *
     * @param distinctExpressions array of expressions
     */
    public void setDistinct(Expression[] distinctExpressions) {
        if (distinct) {
            throw DbException.getUnsupportedException("DISTINCT ON together with DISTINCT");
        }
        this.distinctExpressions = distinctExpressions;
    }

    @Override
    public boolean isAnyDistinct() {
        return distinct || distinctExpressions != null;
    }

    /**
     * Adds a named window definition.
     *
     * @param name   name
     * @param window window definition
     * @return true if a new definition was added, false if old definition was replaced
     */
    public boolean addWindow(String name, Window window) {
        if (windows == null) {
            windows = new HashMap<>();
        }
        return windows.put(name, window) == null;
    }

    /**
     * Returns a window with specified name, or null.
     *
     * @param name name of the window
     * @return the window with specified name, or null
     */
    public Window getWindow(String name) {
        return windows != null ? windows.get(name) : null;
    }

    public void addWhereCondition(Expression condition) {
        if (where == null) {
            where = condition;
        } else {
            where = new ConditionAndOr(ConditionAndOr.AND, condition, where);
        }
    }

    public Expression getWhere() {
        return where;
    }

    private LazyResult queryGroupSorted(int columnCount, ResultTarget result, long offset, boolean quickOffset) {
        LazyResultGroupSorted lazyResult = new LazyResultGroupSorted(expressions, columnCount);
        skipOffset(lazyResult, offset, quickOffset);
        if (result == null) {
            return lazyResult;
        }
        while (lazyResult.next()) {
            result.addRow(lazyResult.currentRow());
        }
        return null;
    }

    /**
     * Create a row with the current values, for queries with group-sort.
     *
     * @param keyValues   the key values
     * @param columnCount the number of columns
     * @return the row
     */
    Value[] createGroupSortedRow(Value[] keyValues, int columnCount) {
        Value[] row = constructGroupResultRow(keyValues, columnCount);
        if (isHavingNullOrFalse(row)) {
            return null;
        }
        return rowForResult(row, columnCount);
    }

    /**
     * Removes HAVING and QUALIFY columns from the row.
     *
     * @param row         the complete row
     * @param columnCount the number of columns to keep
     * @return the same or the truncated row
     */
    private Value[] rowForResult(Value[] row, int columnCount) {
        if (columnCount == resultColumnCount) {
            return row;
        }
        return Arrays.copyOf(row, resultColumnCount);
    }

    private boolean isHavingNullOrFalse(Value[] row) {
        return havingIndex >= 0 && !row[havingIndex].isTrue();
    }

    private Index getGroupSortedIndex() {
        if (groupIndex == null || groupByExpression == null) {
            return null;
        }
        ArrayList<Index> indexes = topTableFilter.getTable().getIndexList();
        if (indexes != null) {
            for (Index index : indexes) {
                if (index.getIndexType().isScan()) {
                    continue;
                }
                if (index.getIndexType().isHash()) {
                    // does not allow scanning entries
                    continue;
                }
                if (isGroupSortedIndex(topTableFilter, index)) {
                    return index;
                }
            }
        }
        return null;
    }

    private boolean isGroupSortedIndex(TableFilter tableFilter, Index index) {
        // check that all the GROUP BY expressions are part of the index
        Column[] indexColumns = index.getColumns();
        // also check that the first columns in the index are grouped
        boolean[] grouped = new boolean[indexColumns.length];
        outerLoop:
        for (int i = 0, size = expressionList.size(); i < size; i++) {
            if (!groupByExpression[i]) {
                continue;
            }
            Expression expr = expressionList.get(i).getNonAliasExpression();
            if (!(expr instanceof ExpressionColumn)) {
                return false;
            }
            ExpressionColumn exprCol = (ExpressionColumn) expr;
            for (int j = 0; j < indexColumns.length; ++j) {
                if (tableFilter == exprCol.getTableFilter()) {
                    if (indexColumns[j].equals(exprCol.getColumn())) {
                        grouped[j] = true;
                        continue outerLoop;
                    }
                }
            }
            // We didn't find a matching index column
            // for one group by expression
            return false;
        }
        // check that the first columns in the index are grouped
        // good: index(a, b, c); group by b, a
        // bad: index(a, b, c); group by a, c
        for (int i = 1; i < grouped.length; i++) {
            if (!grouped[i - 1] && grouped[i]) {
                return false;
            }
        }
        return true;
    }

    boolean isConditionMetForUpdate() {
        if (isConditionMet()) {
            boolean notChanged = true;
            for (TableFilter tableFilter : tableFilters) {
                if (!tableFilter.isJoinOuter() && !tableFilter.isJoinOuterIndirect()) {
                    Row row = tableFilter.get();
                    Table table = tableFilter.getTable();
                    // views, function tables, links, etc. do not support locks
                    if (table.isRowLockable()) {
                        Row lockedRow = table.lockRow(sessionLocal, row);
                        if (lockedRow == null) {
                            return false;
                        }
                        if (!row.hasSharedData(lockedRow)) {
                            tableFilter.set(lockedRow);
                            notChanged = false;
                        }
                    }
                }
            }
            return notChanged || isConditionMet();
        }
        return false;
    }

    boolean isConditionMet() {
        // inner join 的 on 变为 where
        return where == null || where.getBooleanValue(sessionLocal);
    }

    private void queryWindow(int columnCount, LocalResult result, long offset, boolean quickOffset) {
        initGroupData(columnCount);
        try {
            gatherGroup(columnCount, DataAnalysisOperation.STAGE_WINDOW);
            processGroupResult(columnCount, result, offset, quickOffset, false);
        } finally {
            groupData.reset();
        }
    }

    private void queryGroupWindow(int columnCount, LocalResult result, long offset, boolean quickOffset) {
        initGroupData(columnCount);
        try {
            gatherGroup(columnCount, DataAnalysisOperation.STAGE_GROUP);
            try {
                isGroupWindowStage2 = true;
                while (groupData.next() != null) {
                    if (havingIndex < 0 || expressionList.get(havingIndex).getBooleanValue(sessionLocal)) {
                        updateAgg(columnCount, DataAnalysisOperation.STAGE_WINDOW);
                    } else {
                        groupData.remove();
                    }
                }
                groupData.done();
                processGroupResult(columnCount, result, offset, quickOffset, /* Having was performed earlier */ false);
            } finally {
                isGroupWindowStage2 = false;
            }
        } finally {
            groupData.reset();
        }
    }

    private void queryGroup(int columnCount, LocalResult result, long offset, boolean quickOffset) {
        initGroupData(columnCount);
        try {
            gatherGroup(columnCount, DataAnalysisOperation.STAGE_GROUP);
            processGroupResult(columnCount, result, offset, quickOffset, true);
        } finally {
            groupData.reset();
        }
    }

    private void initGroupData(int columnCount) {
        if (groupData == null) {
            setGroupData(SelectGroups.getInstance(sessionLocal, expressionList, isGroupQuery, groupIndex));
        } else {
            updateAgg(columnCount, DataAnalysisOperation.STAGE_RESET);
        }
        groupData.reset();
    }

    void setGroupData(final SelectGroups groupData) {
        this.groupData = groupData;
        topTableFilter.visit(f -> {
            Select s = f.getSelect();
            if (s != null) {
                s.groupData = groupData;
            }
        });
    }

    private void gatherGroup(int columnCount, int stage) {
        long rowNumber = 0;
        setCurrentRowNumber(0);
        while (topTableFilter.next()) {
            setCurrentRowNumber(rowNumber + 1);
            if (isForUpdate ? isConditionMetForUpdate() : isConditionMet()) {
                rowNumber++;
                groupData.nextSource();
                updateAgg(columnCount, stage);
            }
        }
        groupData.done();
    }


    /**
     * Update any aggregate expressions with the query stage.
     *
     * @param columnCount number of columns
     * @param stage       see STAGE_RESET/STAGE_GROUP/STAGE_WINDOW in DataAnalysisOperation
     */
    void updateAgg(int columnCount, int stage) {
        for (int i = 0; i < columnCount; i++) {
            if ((groupByExpression == null || !groupByExpression[i])
                    && (groupByCopies == null || groupByCopies[i] < 0)) {
                Expression expr = expressionList.get(i);
                expr.updateAggregate(sessionLocal, stage);
            }
        }
    }

    private void processGroupResult(int columnCount, LocalResult result, long offset, boolean quickOffset,
                                    boolean withHaving) {
        for (ValueRow currentGroupsKey; (currentGroupsKey = groupData.next()) != null; ) {
            Value[] row = constructGroupResultRow(currentGroupsKey.getList(), columnCount);
            if (withHaving && isHavingNullOrFalse(row)) {
                continue;
            }
            if (qualifyIndex >= 0 && !row[qualifyIndex].isTrue()) {
                continue;
            }
            if (quickOffset && offset > 0) {
                offset--;
                continue;
            }
            result.addRow(rowForResult(row, columnCount));
        }
    }

    private Value[] constructGroupResultRow(Value[] keyValues, int columnCount) {
        Value[] row = new Value[columnCount];
        if (groupIndex != null) {
            for (int i = 0, l = groupIndex.length; i < l; i++) {
                row[groupIndex[i]] = keyValues[i];
            }
        }
        for (int i = 0; i < columnCount; i++) {
            if (groupByExpression != null && groupByExpression[i]) {
                continue;
            }
            if (groupByCopies != null) {
                int original = groupByCopies[i];
                if (original >= 0) {
                    row[i] = row[original];
                    continue;
                }
            }
            row[i] = expressionList.get(i).getValue(sessionLocal);
        }
        return row;
    }

    /**
     * Get the index that matches the ORDER BY list, if one exists. This is to
     * avoid running a separate ORDER BY if an index can be used. This is
     * specially important for large result sets, if only the first few rows are
     * important (LIMIT is used)
     *
     * @return the index if one is found
     */
    private Index getSortIndex() {
        if (sort == null) {
            return null;
        }
        ArrayList<Column> sortColumns = Utils.newSmallArrayList();
        int[] queryColumnIndexes = sort.getQueryColumnIndexes();
        int queryIndexesLength = queryColumnIndexes.length;
        int[] sortIndex = new int[queryIndexesLength];
        for (int i = 0, j = 0; i < queryIndexesLength; i++) {
            int idx = queryColumnIndexes[i];
            if (idx < 0 || idx >= expressionList.size()) {
                throw DbException.getInvalidValueException("ORDER BY", idx + 1);
            }
            Expression expr = expressionList.get(idx);
            expr = expr.getNonAliasExpression();
            if (expr.isConstant()) {
                continue;
            }
            if (!(expr instanceof ExpressionColumn)) {
                return null;
            }
            ExpressionColumn exprCol = (ExpressionColumn) expr;
            if (exprCol.getTableFilter() != topTableFilter) {
                return null;
            }
            sortColumns.add(exprCol.getColumn());
            sortIndex[j++] = i;
        }
        Column[] sortCols = sortColumns.toArray(new Column[0]);
        if (sortCols.length == 0) {
            // sort just on constants - can use scan index
            return topTableFilter.getTable().getScanIndex(sessionLocal);
        }
        ArrayList<Index> list = topTableFilter.getTable().getIndexList();
        if (list != null) {
            int[] sortTypes = sort.getSortTypesWithNullOrdering();
            DefaultNullOrdering defaultNullOrdering = sessionLocal.getDatabase().getDefaultNullOrdering();
            loop:
            for (Index index : list) {
                if (index.getCreateSQL() == null) {
                    // can't use the scan index
                    continue;
                }
                if (index.getIndexType().isHash()) {
                    continue;
                }
                IndexColumn[] indexCols = index.getIndexColumns();
                if (indexCols.length < sortCols.length) {
                    continue;
                }
                for (int j = 0; j < sortCols.length; j++) {
                    // the index and the sort order must start
                    // with the exact same columns
                    IndexColumn idxCol = indexCols[j];
                    Column sortCol = sortCols[j];
                    if (idxCol.column != sortCol) {
                        continue loop;
                    }
                    int sortType = sortTypes[sortIndex[j]];
                    if (sortCol.isNullable()
                            ? defaultNullOrdering.addExplicitNullOrdering(idxCol.sortType) != sortType
                            : (idxCol.sortType & SortOrder.DESCENDING) != (sortType & SortOrder.DESCENDING)) {
                        continue loop;
                    }
                }
                return index;
            }
        }
        if (sortCols.length == 1 && sortCols[0].getId() == -1) {
            // special case: order by _ROWID_
            Index index = topTableFilter.getTable().getScanIndex(sessionLocal);
            if (index.isRowIdIndex()) {
                return index;
            }
        }
        return null;
    }

    private void queryDistinct(ResultTarget result, long offset, long limitRows, boolean withTies,
                               boolean quickOffset) {
        if (limitRows > 0 && offset > 0) {
            limitRows += offset;
            if (limitRows < 0) {
                // Overflow
                limitRows = Long.MAX_VALUE;
            }
        }
        long rowNumber = 0;
        setCurrentRowNumber(0);
        Index index = topTableFilter.getIndex();
        SearchRow first = null;
        int columnIndex = index.getColumns()[0].getId();
        if (!quickOffset) {
            offset = 0;
        }
        while (true) {
            setCurrentRowNumber(++rowNumber);
            Cursor cursor = index.findNext(sessionLocal, first, null);
            if (!cursor.next()) {
                break;
            }
            SearchRow found = cursor.getCurrentSearchRow();
            Value value = found.getValue(columnIndex);
            if (first == null) {
                first = index.getRowFactory().createRow();
            }
            first.setValue(columnIndex, value);
            if (offset > 0) {
                offset--;
                continue;
            }
            result.addRow(value);
            if ((sort == null || sortUsingIndex) && limitRows > 0 && rowNumber >= limitRows && !withTies) {
                break;
            }
        }
    }

    private LazyResult queryFlat(int columnCount,
                                 ResultTarget resultTarget,
                                 long offset,
                                 long limit,
                                 boolean withTies,
                                 boolean quickOffset) {
        if (limit > 0 && offset > 0 && !quickOffset) {
            limit += offset;
            if (limit < 0) { // overflow
                limit = Long.MAX_VALUE;
            }
        }

        LazyResultQueryFlat lazyResultQueryFlat = new LazyResultQueryFlat(expressions, columnCount, isForUpdate);

        skipOffset(lazyResultQueryFlat, offset, quickOffset);

        if (resultTarget == null) {
            return lazyResultQueryFlat;
        }

        if (limit < 0 || sort != null && !sortUsingIndex || withTies && !quickOffset) {
            limit = Long.MAX_VALUE;
        }

        Value[] row = null;
        // limit限度内提前把 lazyResultQueryFlat 内容搬到 resultTarget(localResult)
        while (resultTarget.getRowCount() < limit && lazyResultQueryFlat.next()) {
            row = lazyResultQueryFlat.currentRow();
            resultTarget.addRow(row);
        }

        if (limit != Long.MAX_VALUE && withTies && sort != null && row != null) {
            Value[] expected = row;
            while (lazyResultQueryFlat.next()) {
                row = lazyResultQueryFlat.currentRow();
                if (sort.compare(expected, row) != 0) {
                    break;
                }
                resultTarget.addRow(row);
            }
            resultTarget.limitsWereApplied();
        }

        return null;
    }

    private static void skipOffset(LazyResultSelect lazyResult, long offset, boolean quickOffset) {
        if (quickOffset) {
            while (offset > 0 && lazyResult.skip()) {
                offset--;
            }
        }
    }

    private void queryQuick(int columnCount, ResultTarget result, boolean skipResult) {
        Value[] row = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            Expression expr = expressionList.get(i);
            row[i] = expr.getValue(sessionLocal);
        }
        if (!skipResult) {
            result.addRow(row);
        }
    }

    @Override
    protected ResultInterface queryWithoutCache(long maxRows, ResultTarget resultTarget) {
        disableLazyForJoinSubqueries(topTableFilter);

        OffsetFetch offsetFetch = getOffsetFetch(maxRows);
        long offset = offsetFetch.offset;
        long fetch = offsetFetch.fetch;
        boolean fetchPercent = offsetFetch.fetchPercent;

        boolean lazy =
                sessionLocal.isLazyQueryExecution() &&
                        resultTarget == null &&
                        !isForUpdate &&
                        !isQuickAggregateQuery &&
                        fetch != 0 &&
                        !fetchPercent &&
                        !withTies &&
                        offset == 0
                        && isReadOnly();

        int columnCount = expressionList.size();

        LocalResult localResult = null;

        if (!lazy && (resultTarget == null || !sessionLocal.database.dbSettings.optimizeInsertFromSelect)) {
            localResult = createLocalResult(localResult);
        }

        // Do not add rows before OFFSET to result if possible
        boolean quickOffset = !fetchPercent;
        if (sort != null && (!sortUsingIndex || isAnyDistinct())) {
            localResult = createLocalResult(localResult);
            localResult.setSortOrder(sort);
            if (!sortUsingIndex) {
                quickOffset = false;
            }
        }

        if (distinct) {
            if (!isDistinctQuery) {
                quickOffset = false;
                localResult = createLocalResult(localResult);
                localResult.setDistinct();
            }
        } else if (distinctExpressions != null) {
            quickOffset = false;
            localResult = createLocalResult(localResult);
            localResult.setDistinct(distinctIndexes);
        }

        if (isWindowQuery || isGroupQuery && !isGroupSortedQuery) {
            localResult = createLocalResult(localResult);
        }

        if (!lazy && (fetch >= 0 || offset > 0)) {
            localResult = createLocalResult(localResult);
        }

        topTableFilter.startQuery(sessionLocal);
        topTableFilter.reset();
        topTableFilter.lock(sessionLocal);

        ResultTarget resultTarget0 = localResult != null ? localResult : resultTarget;
        lazy &= resultTarget0 == null;
        LazyResult lazyResult = null;

        if (fetch != 0) {
            // Cannot apply limit now if percent is specified
            long limit = fetchPercent ? -1 : fetch;

            if (isQuickAggregateQuery) {
                queryQuick(columnCount, resultTarget0, quickOffset && offset > 0);
            } else if (isWindowQuery) {
                if (isGroupQuery) {
                    queryGroupWindow(columnCount, localResult, offset, quickOffset);
                } else {
                    queryWindow(columnCount, localResult, offset, quickOffset);
                }
            } else if (isGroupQuery) {
                if (isGroupSortedQuery) {
                    lazyResult = queryGroupSorted(columnCount, resultTarget0, offset, quickOffset);
                } else {
                    queryGroup(columnCount, localResult, offset, quickOffset);
                }
            } else if (isDistinctQuery) {
                queryDistinct(resultTarget0, offset, limit, withTies, quickOffset);
            } else {
                // 内容迁移到了localResult的rows
                lazyResult = queryFlat(columnCount, resultTarget0, offset, limit, withTies, quickOffset);
            }

            if (quickOffset) {
                offset = 0;
            }
        }

        assert lazy == (lazyResult != null) : lazy;

        if (lazyResult != null) { // 是null
            if (fetch > 0) {
                lazyResult.setLimit(fetch);
            }

            if (randomAccessResult) {
                return convertToDistinct(lazyResult);
            }

            return lazyResult;
        }

        if (localResult != null) {
            return finishResult(localResult, offset, fetch, fetchPercent, resultTarget); // resultTarget是null
        }

        return null;
    }

    private void disableLazyForJoinSubqueries(final TableFilter top) {
        if (sessionLocal.isLazyQueryExecution()) {
            top.visit(f -> {
                if (f != top && f.getTable().getTableType() == TableType.VIEW) {
                    QueryExpressionIndex idx = (QueryExpressionIndex) f.getIndex();
                    if (idx != null && idx.getQuery() != null) {
                        idx.getQuery().setNeverLazy(true);
                    }
                }
            });
        }
    }

    private LocalResult createLocalResult(LocalResult old) {
        return old != null ? old : new LocalResult(sessionLocal, expressions, visibleColumnCount, resultColumnCount);
    }

    /**
     * 应对 wildcard
     */
    private void expandColumnList() {
        // the expressionList may change within the loop
        for (int i = 0; i < expressionList.size(); ) {
            Expression expr = expressionList.get(i);
            if (!(expr instanceof Wildcard)) {
                i++;
                continue;
            }

            expressionList.remove(i);

            Wildcard wildcard = (Wildcard) expr;
            String wildcardTableName = wildcard.getTableAlias();
            boolean hasExceptColumns = wildcard.getExceptColumns() != null;
            HashMap<Column, ExpressionColumn> exceptTableColumns = null;

            if (wildcardTableName == null) {
                if (hasExceptColumns) {
                    for (TableFilter filter : tableFilters) {
                        wildcard.mapColumns(filter, 1, Expression.MAP_INITIAL);
                    }
                    exceptTableColumns = wildcard.mapExceptColumns();
                }

                for (TableFilter filter : tableFilters) {
                    i = expandColumnList(filter, i, false, exceptTableColumns);
                }
            } else {

                String schemaName = wildcard.getSchemaName();
                TableFilter wildCardTableFilter = null;

                for (TableFilter tableFilter : tableFilters) {
                    if (sessionLocal.database.equalsIdentifiers(wildcardTableName, tableFilter.getTableAlias())) {
                        if (schemaName == null || sessionLocal.database.equalsIdentifiers(schemaName, tableFilter.getSchemaName())) {
                            if (hasExceptColumns) {
                                wildcard.mapColumns(tableFilter, 1, Expression.MAP_INITIAL);
                                exceptTableColumns = wildcard.mapExceptColumns();
                            }

                            wildCardTableFilter = tableFilter;
                            break;
                        }
                    }
                }

                if (wildCardTableFilter == null) {
                    throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, wildcardTableName);
                }

                i = expandColumnList(wildCardTableFilter, i, true, exceptTableColumns);
            }
        }
    }

    private int expandColumnList(TableFilter tableFilter,
                                 int index,
                                 boolean forAlias,
                                 HashMap<Column, ExpressionColumn> except) {

        String schemaName = tableFilter.getSchemaName();
        String alias = tableFilter.getTableAlias();

        if (forAlias) {
            for (Column c : tableFilter.getTable().getColumns()) {
                index = addExpandedColumn(tableFilter, index, except, schemaName, alias, c);
            }
        } else {
            LinkedHashMap<Column, Column> commonJoinColumns = tableFilter.getCommonJoinColumns();
            if (commonJoinColumns != null) {
                TableFilter replacementFilter = tableFilter.getCommonJoinColumnsFilter();
                String replacementSchema = replacementFilter.getSchemaName();
                String replacementAlias = replacementFilter.getTableAlias();
                for (Entry<Column, Column> entry : commonJoinColumns.entrySet()) {
                    Column left = entry.getKey(), right = entry.getValue();
                    if (!tableFilter.isCommonJoinColumnToExclude(right)
                            && (except == null || except.remove(left) == null && except.remove(right) == null)) {
                        Database database = sessionLocal.getDatabase();
                        Expression e;
                        if (left == right
                                || DataType.hasTotalOrdering(left.getType().getValueType())
                                && DataType.hasTotalOrdering(right.getType().getValueType())) {
                            e = new ExpressionColumn(database, replacementSchema, replacementAlias,
                                    replacementFilter.getColumnName(right));
                        } else {
                            e = new Alias(new CoalesceFunction(CoalesceFunction.COALESCE,
                                    new ExpressionColumn(database, schemaName, alias, tableFilter.getColumnName(left)),
                                    new ExpressionColumn(database, replacementSchema, replacementAlias,
                                            replacementFilter.getColumnName(right))), //
                                    left.getName(), true);
                        }
                        expressionList.add(index++, e);
                    }
                }
            }

            for (Column column : tableFilter.table.columns) {
                if (commonJoinColumns == null || !commonJoinColumns.containsKey(column)) {
                    if (!tableFilter.isCommonJoinColumnToExclude(column)) {
                        index = addExpandedColumn(tableFilter, index, except, schemaName, alias, column);
                    }
                }
            }
        }

        return index;
    }

    private int addExpandedColumn(TableFilter tableFilter,
                                  int index,
                                  HashMap<Column, ExpressionColumn> except,
                                  String schema,
                                  String alias,
                                  Column column) {
        if ((except == null || except.remove(column) == null) && column.getVisible()) {
            ExpressionColumn ec = new ExpressionColumn(sessionLocal.database, schema, alias, tableFilter.getColumnName(column));
            expressionList.add(index++, ec);
        }

        return index;
    }

    @Override
    public void init() {
        if (initialized) {
            throw DbException.getInternalError();
        }

        tableFilters.sort(TableFilter.ORDER_IN_FROM_COMPARATOR);

        // 应对 wildcard
        expandColumnList();

        if ((visibleColumnCount = expressionList.size()) > Constants.MAX_COLUMNS) {
            throw DbException.get(ErrorCode.TOO_MANY_COLUMNS_1, "" + Constants.MAX_COLUMNS);
        }

        ArrayList<String> expressionSQL;
        if (distinctExpressions != null || orderList != null || group != null) {
            expressionSQL = new ArrayList<>(visibleColumnCount);
            for (int i = 0; i < visibleColumnCount; i++) {
                Expression expr = expressionList.get(i);
                expr = expr.getNonAliasExpression();
                expressionSQL.add(expr.getSQL(DEFAULT_SQL_FLAGS, WITHOUT_PARENTHESES));
            }
        } else {
            expressionSQL = null;
        }

        if (distinctExpressions != null) {
            BitSet set = new BitSet();
            for (Expression e : distinctExpressions) {
                set.set(initExpression(expressionSQL, e, false, tableFilters));
            }
            int idx = 0, cnt = set.cardinality();
            distinctIndexes = new int[cnt];
            for (int i = 0; i < cnt; i++) {
                idx = set.nextSetBit(idx);
                distinctIndexes[i] = idx;
                idx++;
            }
        }

        if (orderList != null) {
            initOrder(expressionSQL, isAnyDistinct(), tableFilters);
        }

        resultColumnCount = expressionList.size();

        if (having != null) {
            expressionList.add(having);
            havingIndex = expressionList.size() - 1;
            having = null;
        } else {
            havingIndex = -1;
        }

        if (qualify != null) {
            expressionList.add(qualify);
            qualifyIndex = expressionList.size() - 1;
            qualify = null;
        } else {
            qualifyIndex = -1;
        }

        if (withTies && !hasOrder()) {
            throw DbException.get(ErrorCode.WITH_TIES_WITHOUT_ORDER_BY);
        }

        Database db = sessionLocal.database;

        // first the select list (visible columns),
        // then 'ORDER BY' expressions,
        // then 'HAVING' expressions,
        // and 'GROUP BY' expressions at the end
        if (group != null) {
            int size = group.size();
            int expSize = expressionSQL.size();
            int fullExpSize = expressionList.size();
            if (fullExpSize > expSize) {
                expressionSQL.ensureCapacity(fullExpSize);
                for (int i = expSize; i < fullExpSize; i++) {
                    expressionSQL.add(expressionList.get(i).getSQL(DEFAULT_SQL_FLAGS, WITHOUT_PARENTHESES));
                }
            }
            groupIndex = new int[size];
            for (int i = 0; i < size; i++) {
                Expression expr = group.get(i);
                String sql = expr.getSQL(DEFAULT_SQL_FLAGS, WITHOUT_PARENTHESES);
                int found = -1;
                for (int j = 0; j < expSize; j++) {
                    String s2 = expressionSQL.get(j);
                    if (db.equalsIdentifiers(s2, sql)) {
                        found = mergeGroupByExpressions(db, j, expressionSQL, false);
                        break;
                    }
                }
                if (found < 0) {
                    // special case: GROUP BY a column alias
                    for (int j = 0; j < expSize; j++) {
                        Expression e = expressionList.get(j);
                        if (db.equalsIdentifiers(sql, e.getAlias(sessionLocal, j))) {
                            found = mergeGroupByExpressions(db, j, expressionSQL, true);
                            break;
                        }
                        sql = expr.getAlias(sessionLocal, j);
                        if (db.equalsIdentifiers(sql, e.getAlias(sessionLocal, j))) {
                            found = mergeGroupByExpressions(db, j, expressionSQL, true);
                            break;
                        }
                    }
                }
                if (found < 0) {
                    int index = expressionList.size();
                    groupIndex[i] = index;
                    expressionList.add(expr);
                } else {
                    groupIndex[i] = found;
                }
            }

            checkUsed:
            if (groupByCopies != null) {
                for (int i : groupByCopies) {
                    if (i >= 0) {
                        break checkUsed;
                    }
                }

                groupByCopies = null;
            }

            groupByExpression = new boolean[expressionList.size()];
            for (int gi : groupIndex) {
                groupByExpression[gi] = true;
            }
            group = null;
        }

        // map columns in select list and condition
        for (TableFilter tableFilter : tableFilters) {
            mapColumns(tableFilter, 0);
        }

        mapCondition(havingIndex);
        mapCondition(qualifyIndex);

        initialized = true;
    }

    private void mapCondition(int index) {
        if (index >= 0) {
            Expression expr = expressionList.get(index);
            SelectListColumnResolver res = new SelectListColumnResolver(this);
            expr.mapColumns(res, 0, Expression.MAP_INITIAL);
        }
    }

    private int mergeGroupByExpressions(Database db, int index, ArrayList<String> expressionSQL, //
                                        boolean scanPrevious) {

        /*
         * -1: uniqueness of expression is not known yet
         *
         * -2: expression that is used as a source for a copy or does not have
         * copies
         *
         * >=0: expression is a copy of expression at this index
         */
        if (groupByCopies != null) {
            int c = groupByCopies[index];
            if (c >= 0) {
                return c;
            } else if (c == -2) {
                return index;
            }
        } else {
            groupByCopies = new int[expressionSQL.size()];
            Arrays.fill(groupByCopies, -1);
        }
        String sql = expressionSQL.get(index);
        if (scanPrevious) {
            /*
             * If expression was matched using an alias previous expressions may
             * be identical.
             */
            for (int i = 0; i < index; i++) {
                if (db.equalsIdentifiers(sql, expressionSQL.get(i))) {
                    index = i;
                    break;
                }
            }
        }
        int l = expressionSQL.size();
        for (int i = index + 1; i < l; i++) {
            if (db.equalsIdentifiers(sql, expressionSQL.get(i))) {
                groupByCopies[i] = index;
            }
        }
        groupByCopies[index] = -2;
        return index;
    }

    @Override
    public void prepareExpressions() {
        if (orderList != null) {
            prepareOrder(orderList, expressionList.size());
        }

        ExpressionNames expressionNames = sessionLocal.getMode().expressionNames;
        switch (expressionNames) {
            case ORIGINAL_SQL:
            case POSTGRESQL_STYLE:
                optimizeExpressionsAndPreserveAliases();
                break;
            default:
                expressionList.replaceAll(expression -> expression.optimize(sessionLocal));
        }

        if (sort != null) {
            cleanupOrder();
        }

        if (where != null) {
            where = where.optimizeCondition(sessionLocal);
        }

        if (isGroupQuery &&
                groupIndex == null &&
                havingIndex < 0 &&
                qualifyIndex < 0 &&
                where == null &&
                tableFilters.size() == 1) {

            isQuickAggregateQuery = isEverything(ExpressionVisitor.getOptimizableVisitor(tableFilters.get(0).getTable()));
        }

        expressions = expressionList.toArray(new Expression[0]);
    }

    @Override
    public void preparePlan() {
        if (where != null) {
            for (TableFilter tableFilter : tableFilters) {
                // outer joins: must not add index conditions such as "c is null" - example:
                // create table parent(p int primary key) as select 1;
                // create table child(c int primary key, pc int);
                // insert into child values(2, 1);
                // select p, c from parent
                // left outer join child on p = pc where c is null;
                if (!tableFilter.isJoinOuter() && !tableFilter.isJoinOuterIndirect()) {
                    where.createIndexConditions(sessionLocal, tableFilter);
                }
            }
        }

        cost = preparePlan(sessionLocal.isParsingCreateView());

        if (distinct &&
                sessionLocal.database.dbSettings.optimizeDistinct &&
                !isGroupQuery &&
                tableFilters.size() == 1 &&
                expressionList.size() == 1 &&
                where == null) {
            Expression expr = expressionList.get(0);
            expr = expr.getNonAliasExpression();

            if (expr instanceof ExpressionColumn) {
                Column column = ((ExpressionColumn) expr).getColumn();
                int selectivity = column.getSelectivity();
                Index columnIndex = topTableFilter.getTable().getIndexForColumn(column, false, true);

                if (columnIndex != null &&
                        selectivity != Constants.SELECTIVITY_DEFAULT &&
                        selectivity < 20) {
                    Index current = topTableFilter.getIndex();

                    // if another index is faster
                    if (current == null || current.getIndexType().isScan() || columnIndex == current) {
                        topTableFilter.setIndex(columnIndex);
                        isDistinctQuery = true;
                    }
                }
            }
        }

        if (sort != null && !isQuickAggregateQuery && !isGroupQuery) {
            Index index = getSortIndex();
            Index current = topTableFilter.getIndex();
            if (index != null && current != null) {
                if (current.getIndexType().isScan() || current == index) {
                    topTableFilter.setIndex(index);
                    if (!topTableFilter.hasInComparisons()) {
                        // in(select ...) and in(1,2,3) may return the key in another order
                        sortUsingIndex = true;
                    }
                } else if (index.getIndexColumns() != null
                        && index.getIndexColumns().length >= current
                        .getIndexColumns().length) {
                    IndexColumn[] sortColumns = index.getIndexColumns();
                    IndexColumn[] currentColumns = current.getIndexColumns();
                    boolean swapIndex = false;
                    for (int i = 0; i < currentColumns.length; i++) {
                        if (sortColumns[i].column != currentColumns[i].column) {
                            swapIndex = false;
                            break;
                        }
                        if (sortColumns[i].sortType != currentColumns[i].sortType) {
                            swapIndex = true;
                        }
                    }
                    if (swapIndex) {
                        topTableFilter.setIndex(index);
                        sortUsingIndex = true;
                    }
                }
            }
            if (sortUsingIndex && isForUpdate && !topTableFilter.getIndex().isRowIdIndex()) {
                sortUsingIndex = false;
            }
        }

        if (!isQuickAggregateQuery && isGroupQuery) {
            Index index = getGroupSortedIndex();
            if (index != null) {
                Index current = topTableFilter.getIndex();
                if (current != null && (current.getIndexType().isScan() || current == index)) {
                    topTableFilter.setIndex(index);
                    isGroupSortedQuery = true;
                }
            }
        }

        isPrepared = true;
    }

    private void optimizeExpressionsAndPreserveAliases() {
        for (int i = 0; i < expressionList.size(); i++) {
            Expression original = expressionList.get(i);
            /*
             * TODO cannot evaluate optimized now, because some optimize()
             * methods violate their contract and modify the original
             * expression.
             */
            Expression optimized;
            if (i < visibleColumnCount) {
                String alias = original.getAlias(sessionLocal, i);
                optimized = original.optimize(sessionLocal);
                if (!optimized.getAlias(sessionLocal, i).equals(alias)) {
                    optimized = new Alias(optimized, alias, true);
                }
            } else {
                optimized = original.optimize(sessionLocal);
            }
            expressionList.set(i, optimized);
        }
    }

    @Override
    public double getCost() {
        return cost;
    }

    @Override
    public HashSet<Table> getTables() {
        HashSet<Table> set = new HashSet<>();
        for (TableFilter filter : tableFilters) {
            set.add(filter.getTable());
        }
        return set;
    }

    @Override
    public void fireBeforeSelectTriggers() {
        for (TableFilter filter : tableFilters) {
            filter.getTable().fire(sessionLocal, Trigger.SELECT, true);
        }
    }

    private double preparePlan(boolean parsingCreateView) {
        TableFilter[] topTableFilers = topTableFilters.toArray(new TableFilter[0]);
        for (TableFilter topTableFilter : topTableFilers) {
            topTableFilter.createIndexConditions();
            topTableFilter.setFullCondition(where);
        }

        Optimizer optimizer = new Optimizer(topTableFilers, where, sessionLocal);
        optimizer.optimize(parsingCreateView);

        topTableFilter = optimizer.topTableFilter;
        double planCost = optimizer.cost;

        setEvaluatableRecursive(topTableFilter);

        if (!parsingCreateView) {
            topTableFilter.prepare();
        }

        return planCost;
    }

    private void setEvaluatableRecursive(TableFilter f) {
        for (; f != null; f = f.getJoin()) {
            f.setEvaluatable(f, true);
            if (where != null) {
                where.setEvaluatable(f, true);
            }

            TableFilter n = f.getNestedJoin();
            if (n != null) {
                setEvaluatableRecursive(n);
            }

            Expression on = f.getJoinCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) {
                    // need to check that all added are bound to a table
                    on = on.optimize(sessionLocal);
                    if (!f.isJoinOuter() && !f.isJoinOuterIndirect()) {
                        f.removeJoinCondition();
                        addWhereCondition(on);
                    }
                }
            }

            on = f.getFilterCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) {
                    f.removeFilterCondition();
                    addWhereCondition(on);
                }
            }

            // this is only important for subqueries, so they know
            // the result columns are evaluatable
            for (Expression e : expressionList) {
                e.setEvaluatable(f, true);
            }
        }
    }

    @Override
    public String getPlanSQL(int sqlFlags) {
        // can not use the field sqlStatement because the parameter
        // indexes may be incorrect: ? may be in fact ?2 for a subquery
        // but indexes may be set manually as well
        Expression[] exprList = expressionList.toArray(new Expression[0]);
        StringBuilder builder = new StringBuilder();
        for (TableFilter f : topTableFilters) {
            Table t = f.getTable();
            TableView tableView = t instanceof TableView ? (TableView) t : null;
            if (tableView != null && tableView.isRecursive() && tableView.isTableExpression()) {

                if (!tableView.isTemporary()) {
                    // skip the generation of plan SQL for this already recursive persistent CTEs,
                    // since using a with statement will re-create the common table expression
                    // views.
                } else {
                    builder.append("WITH RECURSIVE ");
                    t.getSchema().getSQL(builder, sqlFlags).append('.');
                    ParserUtil.quoteIdentifier(builder, t.getName(), sqlFlags).append('(');
                    Column.writeColumns(builder, t.getColumns(), sqlFlags);
                    builder.append(") AS ");
                    t.getSQL(builder, sqlFlags).append('\n');
                }
            }
        }
        if (isExplicitTable) {
            builder.append("TABLE ");
            tableFilters.get(0).getPlanSQL(builder, false, sqlFlags);
        } else {
            builder.append("SELECT");
            if (isAnyDistinct()) {
                builder.append(" DISTINCT");
                if (distinctExpressions != null) {
                    Expression.writeExpressions(builder.append(" ON("), distinctExpressions, sqlFlags).append(')');
                }
            }
            for (int i = 0; i < visibleColumnCount; i++) {
                if (i > 0) {
                    builder.append(',');
                }
                builder.append('\n');
                StringUtils.indent(builder, exprList[i].getSQL(sqlFlags, WITHOUT_PARENTHESES), 4, false);
            }
            TableFilter filter = topTableFilter;
            if (filter == null) {
                int count = topTableFilters.size();
                if (count != 1 || !topTableFilters.get(0).isNoFromClauseFilter()) {
                    builder.append("\nFROM ");
                    boolean isJoin = false;
                    for (int i = 0; i < count; i++) {
                        isJoin = getPlanFromFilter(builder, sqlFlags, topTableFilters.get(i), isJoin);
                    }
                }
            } else if (!filter.isNoFromClauseFilter()) {
                getPlanFromFilter(builder.append("\nFROM "), sqlFlags, filter, false);
            }
            if (where != null) {
                getFilterSQL(builder, "\nWHERE ", where, sqlFlags);
            }
            if (groupIndex != null) {
                builder.append("\nGROUP BY ");
                for (int i = 0, l = groupIndex.length; i < l; i++) {
                    if (i > 0) {
                        builder.append(", ");
                    }
                    exprList[groupIndex[i]].getNonAliasExpression().getUnenclosedSQL(builder, sqlFlags);
                }
            } else if (group != null) {
                builder.append("\nGROUP BY ");
                for (int i = 0, l = group.size(); i < l; i++) {
                    if (i > 0) {
                        builder.append(", ");
                    }
                    group.get(i).getUnenclosedSQL(builder, sqlFlags);
                }
            } else emptyGroupingSet:if (isGroupQuery && having == null && havingIndex < 0) {
                for (int i = 0; i < visibleColumnCount; i++) {
                    if (containsAggregate(exprList[i])) {
                        break emptyGroupingSet;
                    }
                }
                builder.append("\nGROUP BY ()");
            }
            getFilterSQL(builder, "\nHAVING ", exprList, having, havingIndex, sqlFlags);
            getFilterSQL(builder, "\nQUALIFY ", exprList, qualify, qualifyIndex, sqlFlags);
        }
        appendEndOfQueryToSQL(builder, sqlFlags, exprList);
        if (isForUpdate) {
            builder.append("\nFOR UPDATE");
        }
        if ((sqlFlags & ADD_PLAN_INFORMATION) != 0) {
            if (isQuickAggregateQuery) {
                builder.append("\n/* direct lookup */");
            }
            if (isDistinctQuery) {
                builder.append("\n/* distinct */");
            }
            if (sortUsingIndex) {
                builder.append("\n/* index sorted */");
            }
            if (isGroupQuery) {
                if (isGroupSortedQuery) {
                    builder.append("\n/* group sorted */");
                }
            }
            // builder.append("\n/* cost: " + cost + " */");
        }
        return builder.toString();
    }

    private static boolean getPlanFromFilter(StringBuilder builder, int sqlFlags, TableFilter f, boolean isJoin) {
        do {
            if (isJoin) {
                builder.append('\n');
            }
            f.getPlanSQL(builder, isJoin, sqlFlags);
            isJoin = true;
        } while ((f = f.getJoin()) != null);
        return isJoin;
    }

    private static void getFilterSQL(StringBuilder builder, String sql, Expression[] exprList, Expression condition,
                                     int conditionIndex, int sqlFlags) {
        if (condition != null) {
            getFilterSQL(builder, sql, condition, sqlFlags);
        } else if (conditionIndex >= 0) {
            getFilterSQL(builder, sql, exprList[conditionIndex], sqlFlags);
        }
    }

    private static void getFilterSQL(StringBuilder builder, String sql, Expression condition, int sqlFlags) {
        condition.getNonAliasExpression().getUnenclosedSQL(builder.append(sql), sqlFlags);
    }

    private static boolean containsAggregate(Expression expression) {
        if (expression instanceof DataAnalysisOperation) {
            if (((DataAnalysisOperation) expression).isAggregate()) {
                return true;
            }
        }
        for (int i = 0, l = expression.getSubexpressionCount(); i < l; i++) {
            if (containsAggregate(expression.getSubexpression(i))) {
                return true;
            }
        }
        return false;
    }

    public void setHaving(Expression having) {
        this.having = having;
    }

    public Expression getHaving() {
        return having;
    }

    public void setQualify(Expression qualify) {
        this.qualify = qualify;
    }

    public Expression getQualify() {
        return qualify;
    }

    public TableFilter getTopTableFilter() {
        return topTableFilter;
    }

    @Override
    public void setForUpdate(boolean b) {
        if (b && (isAnyDistinct() || isGroupQuery)) {
            throw DbException.get(ErrorCode.FOR_UPDATE_IS_NOT_ALLOWED_IN_DISTINCT_OR_GROUPED_SELECT);
        }
        this.isForUpdate = b;
    }

    @Override
    public void mapColumns(ColumnResolver columnResolver, int level) {
        for (Expression expression : expressionList) {
            expression.mapColumns(columnResolver, level, Expression.MAP_INITIAL);
        }

        if (where != null) {
            where.mapColumns(columnResolver, level, Expression.MAP_INITIAL);
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for (Expression e : expressionList) {
            e.setEvaluatable(tableFilter, b);
        }
        if (where != null) {
            where.setEvaluatable(tableFilter, b);
        }
    }

    /**
     * Check if this is an aggregate query with direct lookup, for example a
     * query of the type SELECT COUNT(*) FROM TEST or
     * SELECT MAX(ID) FROM TEST.
     *
     * @return true if a direct lookup is possible
     */
    public boolean isQuickAggregateQuery() {
        return isQuickAggregateQuery;
    }

    /**
     * Checks if this query is a group query.
     *
     * @return whether this query is a group query.
     */
    public boolean isGroupQuery() {
        return isGroupQuery;
    }

    /**
     * Checks if this query contains window functions.
     *
     * @return whether this query contains window functions
     */
    public boolean isWindowQuery() {
        return isWindowQuery;
    }

    /**
     * Checks if window stage of group window query is performed. If true,
     * column resolver may not be used.
     *
     * @return true if window stage of group window query is performed
     */
    public boolean isGroupWindowStage2() {
        return isGroupWindowStage2;
    }

    @Override
    public void addGlobalCondition(Parameter param, int columnId, int comparisonType) {
        addParameter(param);
        Expression comp;
        Expression col = expressionList.get(columnId);
        col = col.getNonAliasExpression();
        if (col.isEverything(ExpressionVisitor.QUERY_COMPARABLE_VISITOR)) {
            comp = new Comparison(comparisonType, col, param, false);
        } else {
            // this condition will always evaluate to true, but need to
            // add the parameter, so it can be set later
            comp = new Comparison(Comparison.EQUAL_NULL_SAFE, param, param, false);
        }
        comp = comp.optimize(sessionLocal);
        if (isWindowQuery) {
            qualify = addGlobalCondition(qualify, comp);
        } else if (isGroupQuery) {
            for (int i = 0; groupIndex != null && i < groupIndex.length; i++) {
                if (groupIndex[i] == columnId) {
                    where = addGlobalCondition(where, comp);
                    return;
                }
            }
            if (havingIndex >= 0) {
                having = expressionList.get(havingIndex);
            }
            having = addGlobalCondition(having, comp);
        } else {
            where = addGlobalCondition(where, comp);
        }
    }

    private static Expression addGlobalCondition(Expression condition, Expression additional) {
        if (!(condition instanceof ConditionLocalAndGlobal)) {
            return new ConditionLocalAndGlobal(condition, additional);
        }
        Expression oldLocal, oldGlobal;
        if (condition.getSubexpressionCount() == 1) {
            oldLocal = null;
            oldGlobal = condition.getSubexpression(0);
        } else {
            oldLocal = condition.getSubexpression(0);
            oldGlobal = condition.getSubexpression(1);
        }
        return new ConditionLocalAndGlobal(oldLocal, new ConditionAndOr(ConditionAndOr.AND, oldGlobal, additional));
    }

    @Override
    public void updateAggregate(SessionLocal s, int stage) {
        for (Expression e : expressionList) {
            e.updateAggregate(s, stage);
        }
        if (where != null) {
            where.updateAggregate(s, stage);
        }
        if (having != null) {
            having.updateAggregate(s, stage);
        }
        if (qualify != null) {
            qualify.updateAggregate(s, stage);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor expressionVisitor) {
        switch (expressionVisitor.type) {
            case ExpressionVisitor.DETERMINISTIC: {
                if (isForUpdate) {
                    return false;
                }

                for (TableFilter f : tableFilters) {
                    if (!f.getTable().isDeterministic()) {
                        return false;
                    }
                }

                break;
            }
            case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID: {
                for (TableFilter f : tableFilters) {
                    long m = f.getTable().getMaxDataModificationId();
                    expressionVisitor.addDataModificationId(m);
                }

                break;
            }
            case ExpressionVisitor.EVALUATABLE: {
                if (!sessionLocal.getDatabase().getSettings().optimizeEvaluatableSubqueries) {
                    return false;
                }

                break;
            }
            case ExpressionVisitor.GET_DEPENDENCIES: {
                for (TableFilter tableFilter : tableFilters) {
                    expressionVisitor.addDependency(tableFilter.table);
                    tableFilter.table.addDependencies(expressionVisitor.getDependencies());
                }

                break;
            }
            default:
        }

        ExpressionVisitor v2 = expressionVisitor.incrementQueryLevel(1);

        for (Expression e : expressionList) {
            if (!e.isEverything(v2)) {
                return false;
            }
        }

        if (where != null && !where.isEverything(v2)) {
            return false;
        }

        if (having != null && !having.isEverything(v2)) {
            return false;
        }

        return qualify == null || qualify.isEverything(v2);
    }


    @Override
    public boolean isCacheable() {
        return !isForUpdate;
    }

    @Override
    public boolean allowGlobalConditions() {
        return offsetExpr == null && fetchExpr == null && distinctExpressions == null;
    }

    public SortOrder getSortOrder() {
        return sort;
    }

    @Override
    public boolean isConstantQuery() {
        if (!super.isConstantQuery() || distinctExpressions != null || where != null || isGroupQuery
                || isWindowQuery || !isNoFromClause()) {
            return false;
        }
        for (int i = 0; i < visibleColumnCount; i++) {
            if (!expressionList.get(i).isConstant()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Expression getIfSingleRow() {
        if (offsetExpr != null || fetchExpr != null || where != null || isGroupQuery || isWindowQuery
                || !isNoFromClause()) {
            return null;
        }
        if (visibleColumnCount == 1) {
            return expressionList.get(0);
        }
        Expression[] array = new Expression[visibleColumnCount];
        for (int i = 0; i < visibleColumnCount; i++) {
            array[i] = expressionList.get(i);
        }
        return new ExpressionList(array, false);
    }

    private boolean isNoFromClause() {
        if (topTableFilter != null) {
            return topTableFilter.isNoFromClauseFilter();
        } else if (topTableFilters.size() == 1) {
            return topTableFilters.get(0).isNoFromClauseFilter();
        }
        return false;
    }

    /**
     * Lazy execution for this select.
     */
    private abstract class LazyResultSelect extends LazyResult {

        long rowNumber;
        int columnCount;

        LazyResultSelect(Expression[] expressions, int columnCount) {
            super(getSessionLocal(), expressions);
            this.columnCount = columnCount;
            setCurrentRowNumber(0);
        }

        @Override
        public final int getVisibleColumnCount() {
            return visibleColumnCount;
        }

        @Override
        public void reset() {
            super.reset();
            topTableFilter.reset();
            setCurrentRowNumber(0);
            rowNumber = 0;
        }
    }

    /**
     * Lazy execution for a flat query.
     */
    private final class LazyResultQueryFlat extends LazyResultSelect {

        private final boolean forUpdate;

        LazyResultQueryFlat(Expression[] expressions, int columnCount, boolean forUpdate) {
            super(expressions, columnCount);
            this.forUpdate = forUpdate;
        }

        @Override
        protected Value[] fetchNextRow() {
            while (topTableFilter.next()) {
                setCurrentRowNumber(rowNumber + 1);

                // This method may lock rows
                if (forUpdate ? isConditionMetForUpdate() : isConditionMet()) {
                    ++rowNumber;

                    Value[] row = new Value[columnCount];

                    for (int i = 0; i < columnCount; i++) {
                        row[i] = expressionList.get(i).getValue(sessionLocal);
                    }

                    return row;
                }
            }

            return null;
        }

        @Override
        protected boolean skipNextRow() {
            while (topTableFilter.next()) {
                setCurrentRowNumber(rowNumber + 1);
                // This method does not lock rows
                if (isConditionMet()) {
                    ++rowNumber;
                    return true;
                }
            }
            return false;
        }

    }

    /**
     * Lazy execution for a group sorted query.
     */
    private final class LazyResultGroupSorted extends LazyResultSelect {

        private Value[] previousKeyValues;

        LazyResultGroupSorted(Expression[] expressions, int columnCount) {
            super(expressions, columnCount);
            if (groupData == null) {
                setGroupData(SelectGroups.getInstance(getSessionLocal(), Select.this.expressionList, isGroupQuery,
                        groupIndex));
            } else {
                updateAgg(columnCount, DataAnalysisOperation.STAGE_RESET);
                groupData.resetLazy();
            }
        }

        @Override
        public void reset() {
            super.reset();
            groupData.resetLazy();
            previousKeyValues = null;
        }

        @Override
        protected Value[] fetchNextRow() {
            while (topTableFilter.next()) {
                setCurrentRowNumber(rowNumber + 1);
                if (isConditionMet()) {
                    rowNumber++;
                    int groupSize = groupIndex.length;
                    Value[] keyValues = new Value[groupSize];
                    // update group
                    for (int i = 0; i < groupSize; i++) {
                        int idx = groupIndex[i];
                        Expression expr = expressionList.get(idx);
                        keyValues[i] = expr.getValue(getSessionLocal());
                    }

                    Value[] row = null;
                    if (previousKeyValues == null) {
                        previousKeyValues = keyValues;
                        groupData.nextLazyGroup();
                    } else {
                        SessionLocal session = getSessionLocal();
                        for (int i = 0; i < groupSize; i++) {
                            if (session.compare(previousKeyValues[i], keyValues[i]) != 0) {
                                row = createGroupSortedRow(previousKeyValues, columnCount);
                                previousKeyValues = keyValues;
                                groupData.nextLazyGroup();
                                break;
                            }
                        }
                    }
                    groupData.nextLazyRow();
                    updateAgg(columnCount, DataAnalysisOperation.STAGE_GROUP);
                    if (row != null) {
                        return row;
                    }
                }
            }
            Value[] row = null;
            if (previousKeyValues != null) {
                row = createGroupSortedRow(previousKeyValues, columnCount);
                previousKeyValues = null;
            }
            return row;
        }
    }

}
