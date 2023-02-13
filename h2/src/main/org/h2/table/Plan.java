/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.message.Trace;

/**
 * A possible query execution plan. The time required to execute a query depends
 * on the order the tables are accessed.
 */
public class Plan {

    public final TableFilter[] tableFilters;
    public final HashMap<TableFilter, PlanItem> tableFilter_planItem = new HashMap<>();
    private final Expression[] allConditions;
    private final TableFilter[] allTableFilters;

    /**
     * Create a query plan with the given order.
     *
     * @param tableFilters the tables of the query
     * @param count        the number of table items
     * @param condition    the condition in the WHERE clause
     */
    public Plan(TableFilter[] tableFilters, int count, Expression condition) {
        this.tableFilters = new TableFilter[count];
        System.arraycopy(tableFilters, 0, this.tableFilters, 0, count);

        ArrayList<Expression> allConditionList = new ArrayList<>();
        ArrayList<TableFilter> allTableFilterList = new ArrayList<>();

        if (condition != null) {
            allConditionList.add(condition);
        }

        for (TableFilter tableFilter : this.tableFilters) {
            tableFilter.visit(f1 -> {
                allTableFilterList.add(f1);

                if (f1.getJoinCondition() != null) {
                    allConditionList.add(f1.getJoinCondition());
                }
            });
        }

        allConditions = allConditionList.toArray(new Expression[0]);
        allTableFilters = allTableFilterList.toArray(new TableFilter[0]);
    }

    /**
     * Get the plan item for the given table.
     *
     * @param filter the table
     * @return the plan item
     */
    public PlanItem getItem(TableFilter filter) {
        return tableFilter_planItem.get(filter);
    }

    /**
     * The the list of tables.
     *
     * @return the list of tables
     */
    public TableFilter[] getTableFilters() {
        return tableFilters;
    }

    /**
     * Remove all index conditions that can not be used.
     */
    public void removeUnusableIndexConditions() {
        for (int i = 0; i < allTableFilters.length; i++) {
            TableFilter f = allTableFilters[i];
            setEvaluatable(f, true);
            if (i < allTableFilters.length - 1) {
                // the last table doesn't need the optimization,
                // otherwise the expression is calculated twice unnecessarily
                // (not that bad but not optimal)
                f.optimizeFullCondition();
            }
            f.removeUnusableIndexConditions();
        }
        for (TableFilter f : allTableFilters) {
            setEvaluatable(f, false);
        }
    }

    /**
     * Calculate the cost of this query plan.
     *
     * @param sessionLocal      the session
     * @param allColumnsForPlan calculates all columns on-demand
     * @return the cost
     */
    public double calculateCost(SessionLocal sessionLocal, AllColumnsForPlan allColumnsForPlan) {
        Trace t = sessionLocal.getTrace();
        if (t.isDebugEnabled()) {
            t.debug("Plan: calculate cost for plan {0}", Arrays.toString(allTableFilters));
        }

        double cost = 1;
        boolean invalidPlan = false;

        for (int i = 0; i < allTableFilters.length; i++) {
            TableFilter tableFilter = allTableFilters[i];
            if (t.isDebugEnabled()) {
                t.debug("Plan:for table filter {0}", tableFilter);
            }

            // 针对了各个的表计算scan和index中的的成本小的
            PlanItem planItem = tableFilter.getBestPlanItem(sessionLocal, allTableFilters, i, allColumnsForPlan);
            tableFilter_planItem.put(tableFilter, planItem);

            if (t.isDebugEnabled()) {
                t.debug("Plan:best plan item cost {0} index {1}", planItem.cost, planItem.getIndex().getPlanSQL());
            }

            cost += cost * planItem.cost;
            setEvaluatable(tableFilter, true);

            Expression on = tableFilter.getJoinCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) {
                    invalidPlan = true;
                    break;
                }
            }
        }

        if (invalidPlan) {
            cost = Double.POSITIVE_INFINITY;
        }

        if (t.isDebugEnabled()) {
            sessionLocal.getTrace().debug("Plan: plan cost {0}", cost);
        }

        for (TableFilter f : allTableFilters) {
            setEvaluatable(f, false);
        }

        return cost;
    }

    private void setEvaluatable(TableFilter filter, boolean b) {
        filter.setEvaluatable(filter, b);
        for (Expression e : allConditions) {
            e.setEvaluatable(filter, b);
        }
    }
}
