/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.command.Command;
import org.h2.command.CommandInterface;
import org.h2.command.query.Query;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.expression.ValueExpression;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.mvstore.db.MVPrimaryIndex;
import org.h2.result.ResultInterface;
import org.h2.result.ResultTarget;
import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.table.DataChangeDeltaTable;
import org.h2.table.DataChangeDeltaTable.ResultOption;
import org.h2.table.Table;
import org.h2.util.HasSQL;
import org.h2.value.Value;

/**
 * This class represents the statement
 * INSERT
 */
public final class Insert extends CommandWithValues implements ResultTarget {

    private Table table;
    private Column[] columns;
    private Query query;
    // insert了多少row
    private long insertedRowNum;
    private boolean insertFromSelect;

    private Boolean overridingSystem;

    /**
     * For MySQL-style INSERT ... ON DUPLICATE KEY UPDATE ....
     */
    private HashMap<Column, Expression> duplicateKeyAssignmentMap;

    private Value[] onDuplicateKeyRow;

    /**
     * For MySQL-style INSERT IGNORE and PostgreSQL-style ON CONFLICT DO
     * NOTHING.
     */
    private boolean ignore;

    private ResultTarget deltaChangeCollector;

    private ResultOption deltaChangeCollectionMode;

    public Insert(SessionLocal session) {
        super(session);
    }

    @Override
    public void setCommand(Command command) {
        super.setCommand(command);
        if (query != null) {
            query.setCommand(command);
        }
    }

    @Override
    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
    }

    /**
     * Sets MySQL-style INSERT IGNORE mode or PostgreSQL-style ON CONFLICT
     * DO NOTHING.
     *
     * @param ignore ignore duplicates
     */
    public void setIgnore(boolean ignore) {
        this.ignore = ignore;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public void setOverridingSystem(Boolean overridingSystem) {
        this.overridingSystem = overridingSystem;
    }

    /**
     * Keep a collection of the columns to pass to update if a duplicate key
     * happens, for MySQL-style INSERT ... ON DUPLICATE KEY UPDATE ....
     *
     * @param column the column
     * @param expression the expression
     */
    public void addAssignmentForDuplicate(Column column, Expression expression) {
        if (duplicateKeyAssignmentMap == null) {
            duplicateKeyAssignmentMap = new HashMap<>();
        }
        if (duplicateKeyAssignmentMap.putIfAbsent(column, expression) != null) {
            throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, column.getName());
        }
    }

    @Override
    public long update(ResultTarget resultTarget, ResultOption resultOption) {
        this.deltaChangeCollector = resultTarget;
        this.deltaChangeCollectionMode = resultOption;
        try {
            return insertRows();
        } finally {
            this.deltaChangeCollector = null;
            this.deltaChangeCollectionMode = null;
        }
    }

    private long insertRows() {
        sessionLocal.getUser().checkTableRight(table, Right.INSERT);

        setCurrentRowNumber(0);

        table.fire(sessionLocal, Trigger.INSERT, true);

        insertedRowNum = 0;

        int listSize = valuesExpressionList.size();

        // insert into table values (1) 对应, valuesExpressionList 对应values内容
        // valuesExpressionList的元素对应要insert 单个的row的内容
        if (listSize > 0) { // listSize 对应要insert的row数量
            int columnLen = columns.length;

            // 遍历要insert的各个row
            for (int x = 0; x < listSize; x++) {
                Row newRow = table.getTemplateRow();

                Expression[] expressions = valuesExpressionList.get(x);

                setCurrentRowNumber(x + 1);

                // 填充单个row内容 遍历了insert牵涉到的column
                for (int i = 0; i < columnLen; i++) {
                    Column column = columns[i];

                    int columnId = column.id;

                    Expression expression = expressions[i];
                    if (expression == ValueExpression.DEFAULT) {
                       continue;
                    }

                    try {
                        newRow.setValue(columnId, expression.getValue(sessionLocal));
                    } catch (DbException ex) {
                        throw setRow(ex, x, getSimpleSQL(expressions));
                    }
                }

                insertedRowNum++;

                table.convertInsertRow(sessionLocal, newRow, overridingSystem);

                if (deltaChangeCollectionMode == ResultOption.NEW) {
                    deltaChangeCollector.addRow(newRow.getValueList().clone());
                }

                // 要未设置trigger的话得到false
                if (!table.fireBeforeRow(sessionLocal, null, newRow)) {
                    table.lock(sessionLocal, Table.WRITE_LOCK);

                    try {
                        table.addRow(sessionLocal, newRow);
                    } catch (DbException de) {
                        if (handleOnDuplicate(de, null)) {
                            // MySQL returns 2 for updated row
                            // TODO: detect no-op change
                            insertedRowNum++;
                        } else {
                            // INSERT IGNORE case
                            insertedRowNum--;
                        }

                        continue;
                    }

                    DataChangeDeltaTable.collectInsertedFinalRow(sessionLocal, table, deltaChangeCollector, deltaChangeCollectionMode, newRow);

                    table.fireAfterRow(sessionLocal, null, newRow, false);
                } else {
                    DataChangeDeltaTable.collectInsertedFinalRow(sessionLocal, table, deltaChangeCollector, deltaChangeCollectionMode, newRow);
                }
            }
        } else {
            table.lock(sessionLocal, Table.WRITE_LOCK);

            if (insertFromSelect) {
                query.query(0, this);
            } else {
                ResultInterface rows = query.query(0);
                while (rows.next()) {
                    Value[] r = rows.currentRow();
                    try {
                        addRow(r);
                    } catch (DbException de) {
                        if (handleOnDuplicate(de, r)) {
                            // MySQL returns 2 for updated row
                            // TODO: detect no-op change
                            insertedRowNum++;
                        } else {
                            // INSERT IGNORE case
                            insertedRowNum--;
                        }
                    }
                }
                rows.close();
            }
        }

        table.fire(sessionLocal, Trigger.INSERT, false);

        return insertedRowNum;
    }

    @Override
    public void addRow(Value... values) {
        Row newRow = table.getTemplateRow();
        setCurrentRowNumber(++insertedRowNum);
        for (int j = 0, len = columns.length; j < len; j++) {
            newRow.setValue(columns[j].getId(), values[j]);
        }
        table.convertInsertRow(sessionLocal, newRow, overridingSystem);
        if (deltaChangeCollectionMode == ResultOption.NEW) {
            deltaChangeCollector.addRow(newRow.getValueList().clone());
        }
        if (!table.fireBeforeRow(sessionLocal, null, newRow)) {
            table.addRow(sessionLocal, newRow);
            DataChangeDeltaTable.collectInsertedFinalRow(sessionLocal, table, deltaChangeCollector,
                    deltaChangeCollectionMode, newRow);
            table.fireAfterRow(sessionLocal, null, newRow, false);
        } else {
            DataChangeDeltaTable.collectInsertedFinalRow(sessionLocal, table, deltaChangeCollector,
                    deltaChangeCollectionMode, newRow);
        }
    }

    @Override
    public long getRowCount() {
        // This method is not used in this class
        return insertedRowNum;
    }

    @Override
    public void limitsWereApplied() {
        // Nothing to do
    }

    @Override
    public String getPlanSQL(int sqlFlags) {
        StringBuilder builder = new StringBuilder("INSERT INTO ");
        table.getSQL(builder, sqlFlags).append('(');
        Column.writeColumns(builder, columns, sqlFlags);
        builder.append(")\n");
        if (insertFromSelect) {
            builder.append("DIRECT ");
        }
        if (!valuesExpressionList.isEmpty()) {
            builder.append("VALUES ");
            int row = 0;
            if (valuesExpressionList.size() > 1) {
                builder.append('\n');
            }
            for (Expression[] expr : valuesExpressionList) {
                if (row++ > 0) {
                    builder.append(",\n");
                }
                Expression.writeExpressions(builder.append('('), expr, sqlFlags).append(')');
            }
        } else {
            builder.append(query.getPlanSQL(sqlFlags));
        }
        return builder.toString();
    }

    @Override
    void doPrepare() {
        if (columns == null) {
            if (!valuesExpressionList.isEmpty() && valuesExpressionList.get(0).length == 0) {
                // special case where table is used as a sequence
                columns = new Column[0];
            } else {
                columns = table.getColumns();
            }
        }
        if (!valuesExpressionList.isEmpty()) {
            for (Expression[] expr : valuesExpressionList) {
                if (expr.length != columns.length) {
                    throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
                }
                for (int i = 0, len = expr.length; i < len; i++) {
                    Expression e = expr[i];
                    if (e != null) {
                        e = e.optimize(sessionLocal);
                        if (e instanceof Parameter) {
                            Parameter p = (Parameter) e;
                            p.setColumn(columns[i]);
                        }
                        expr[i] = e;
                    }
                }
            }
        } else {
            query.prepare();
            if (query.getColumnCount() != columns.length) {
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
        }
    }

    @Override
    public int getType() {
        return CommandInterface.INSERT;
    }

    @Override
    public String getStatementName() {
        return "INSERT";
    }

    public void setInsertFromSelect(boolean value) {
        this.insertFromSelect = value;
    }

    @Override
    public boolean isCacheable() {
        return duplicateKeyAssignmentMap == null;
    }

    /**
     * @param de duplicate key exception
     * @param currentRow current row values (optional)
     * @return {@code true} if row was updated, {@code false} if row was ignored
     */
    private boolean handleOnDuplicate(DbException de, Value[] currentRow) {
        if (de.getErrorCode() != ErrorCode.DUPLICATE_KEY_1) {
            throw de;
        }
        if (duplicateKeyAssignmentMap == null) {
            if (ignore) {
                return false;
            }
            throw de;
        }

        int columnCount = columns.length;
        Expression[] row = (currentRow == null) ? valuesExpressionList.get((int) getCurrentRowNumber() - 1)
                : new Expression[columnCount];
        onDuplicateKeyRow = new Value[table.getColumns().length];
        for (int i = 0; i < columnCount; i++) {
            Value value;
            if (currentRow != null) {
                value = currentRow[i];
                row[i] = ValueExpression.get(value);
            } else {
                value = row[i].getValue(sessionLocal);
            }
            onDuplicateKeyRow[columns[i].getId()] = value;
        }

        StringBuilder builder = new StringBuilder("UPDATE ");
        table.getSQL(builder, HasSQL.DEFAULT_SQL_FLAGS).append(" SET ");
        boolean f = false;
        for (Entry<Column, Expression> entry : duplicateKeyAssignmentMap.entrySet()) {
            if (f) {
                builder.append(", ");
            }
            f = true;
            entry.getKey().getSQL(builder, HasSQL.DEFAULT_SQL_FLAGS).append('=');
            entry.getValue().getUnenclosedSQL(builder, HasSQL.DEFAULT_SQL_FLAGS);
        }
        builder.append(" WHERE ");
        Index foundIndex = (Index) de.getSource();
        if (foundIndex == null) {
            throw DbException.getUnsupportedException(
                    "Unable to apply ON DUPLICATE KEY UPDATE, no index found!");
        }
        prepareUpdateCondition(foundIndex, row).getUnenclosedSQL(builder, HasSQL.DEFAULT_SQL_FLAGS);
        String sql = builder.toString();
        Update command = (Update) sessionLocal.prepare(sql);
        command.setOnDuplicateKeyInsert(this);
        for (Parameter param : command.getParameterList()) {
            Parameter insertParam = parameterList.get(param.getIndex());
            param.setValue(insertParam.getValue(sessionLocal));
        }
        boolean result = command.update() > 0;
        onDuplicateKeyRow = null;
        return result;
    }

    private Expression prepareUpdateCondition(Index foundIndex, Expression[] row) {
        // MVPrimaryIndex is playing fast and loose with it's implementation of
        // the Index interface.
        // It returns all of the columns in the table when we call
        // getIndexColumns() or getColumns().
        // Don't have time right now to fix that, so just special-case it.
        // PageDataIndex has the same problem.
        final Column[] indexedColumns;
        if (foundIndex instanceof MVPrimaryIndex) {
            MVPrimaryIndex foundMV = (MVPrimaryIndex) foundIndex;
            indexedColumns = new Column[] { foundMV.getIndexColumns()[foundMV
                    .getMainIndexColumn()].column };
        } else {
            indexedColumns = foundIndex.getColumns();
        }

        Expression condition = null;
        for (Column column : indexedColumns) {
            ExpressionColumn expr = new ExpressionColumn(sessionLocal.getDatabase(),
                    table.getSchema().getName(), table.getName(), column.getName());
            for (int i = 0; i < columns.length; i++) {
                if (expr.getColumnName(sessionLocal, i).equals(columns[i].getName())) {
                    if (condition == null) {
                        condition = new Comparison(Comparison.EQUAL, expr, row[i], false);
                    } else {
                        condition = new ConditionAndOr(ConditionAndOr.AND, condition,
                                new Comparison(Comparison.EQUAL, expr, row[i], false));
                    }
                    break;
                }
            }
        }
        return condition;
    }

    /**
     * Get the value to use for the specified column in case of a duplicate key.
     *
     * @param columnIndex the column index
     * @return the value
     */
    public Value getOnDuplicateKeyValue(int columnIndex) {
        return onDuplicateKeyRow[columnIndex];
    }

    @Override
    public void collectDependencies(HashSet<DbObject> dependencies) {
        ExpressionVisitor visitor = ExpressionVisitor.getDependenciesVisitor(dependencies);
        if (!valuesExpressionList.isEmpty()) {
            for (Expression[] expr : valuesExpressionList) {
                for (Expression e : expr) {
                    e.isEverything(visitor);
                }
            }
        } else {
            query.isEverything(visitor);
        }
    }

}
