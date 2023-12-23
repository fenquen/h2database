/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.HashSet;

import org.h2.api.Trigger;
import org.h2.command.CommandInterface;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.result.ResultTarget;
import org.h2.result.Row;
import org.h2.table.DataChangeDeltaTable.ResultOption;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This class represents the statement DELETE
 */
public final class Delete extends FilteredDataChangeStatement {

    public Delete(SessionLocal sessionLocal) {
        super(sessionLocal);
    }

    @Override
    public long update(ResultTarget deltaChangeCollector, ResultOption deltaChangeCollectionMode) {
        targetTableFilter.startQuery(sessionLocal);
        targetTableFilter.reset();
        Table table = targetTableFilter.getTable();
        sessionLocal.getUser().checkTableRight(table, Right.DELETE);
        table.fire(sessionLocal, Trigger.DELETE, true);
        table.lock(sessionLocal, Table.WRITE_LOCK);
        long limitRows = -1;
        if (fetchExpr != null) {
            Value v = fetchExpr.getValue(sessionLocal);
            if (v == ValueNull.INSTANCE || (limitRows = v.getLong()) < 0) {
                throw DbException.getInvalidValueException("FETCH", v);
            }
        }
        try (LocalResult rows = LocalResult.forTable(sessionLocal, table)) {
            setCurrentRowNumber(0);
            long count = 0;
            while (nextRow(limitRows, count)) {
                Row row = targetTableFilter.get();
                if (table.isRowLockable()) {
                    Row lockedRow = table.lockRow(sessionLocal, row);
                    if (lockedRow == null) {
                        continue;
                    }
                    if (!row.hasSharedData(lockedRow)) {
                        row = lockedRow;
                        targetTableFilter.set(row);
                        if (condition != null && !condition.getBooleanValue(sessionLocal)) {
                            continue;
                        }
                    }
                }
                if (deltaChangeCollectionMode == ResultOption.OLD) {
                    deltaChangeCollector.addRow(row.getValueList());
                }
                if (!table.fireRow() || !table.fireBeforeRow(sessionLocal, row, null)) {
                    rows.addRowForTable(row);
                }
                count++;
            }
            rows.done();
            long rowScanCount = 0;
            while (rows.next()) {
                if ((++rowScanCount & 127) == 0) {
                    checkCanceled();
                }
                Row row = rows.currentRowForTable();
                table.removeRow(sessionLocal, row);
            }
            if (table.fireRow()) {
                for (rows.reset(); rows.next();) {
                    table.fireAfterRow(sessionLocal, rows.currentRowForTable(), null, false);
                }
            }
            table.fire(sessionLocal, Trigger.DELETE, false);
            return count;
        }
    }

    @Override
    public String getPlanSQL(int sqlFlags) {
        StringBuilder builder = new StringBuilder("DELETE FROM ");
        targetTableFilter.getPlanSQL(builder, false, sqlFlags);
        appendFilterCondition(builder, sqlFlags);
        return builder.toString();
    }

    @Override
    void doPrepare() {
        if (condition != null) {
            condition.mapColumns(targetTableFilter, 0, Expression.MAP_INITIAL);
            condition = condition.optimizeCondition(sessionLocal);
            if (condition != null) {
                condition.createIndexConditions(sessionLocal, targetTableFilter);
            }
        }
        TableFilter[] filters = new TableFilter[] { targetTableFilter };
        PlanItem item = targetTableFilter.getBestPlanItem(sessionLocal, filters, 0, new AllColumnsForPlan(filters));
        targetTableFilter.setPlanItem(item);
        targetTableFilter.prepare();
    }

    @Override
    public int getType() {
        return CommandInterface.DELETE;
    }

    @Override
    public String getStatementName() {
        return "DELETE";
    }

    @Override
    public void collectDependencies(HashSet<DbObject> dependencies) {
        ExpressionVisitor visitor = ExpressionVisitor.getDependenciesVisitor(dependencies);
        if (condition != null) {
            condition.isEverything(visitor);
        }
    }

}
