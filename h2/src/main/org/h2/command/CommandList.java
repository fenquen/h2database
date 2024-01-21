/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.h2.engine.DbObject;
import org.h2.engine.SessionLocal;
import org.h2.expression.Parameter;
import org.h2.expression.ParameterInterface;
import org.h2.result.ResultInterface;
import org.h2.result.ResultWithGeneratedKeys;

/**
 * Represents a list of SQL statements.
 */
class CommandList extends Command {

    private final CommandContainer commandContainer;
    private final ArrayList<Prepared> preparedList;
    private final ArrayList<Parameter> parameterList;
    private String remaining;
    private Command remainingCommand;

    CommandList(SessionLocal session,
                String sql,
                CommandContainer commandContainer,
                ArrayList<Prepared> preparedList,
                ArrayList<Parameter> parameterList,
                String remaining) {
        super(session, sql);

        this.commandContainer = commandContainer;
        this.preparedList = preparedList;
        this.parameterList = parameterList;
        this.remaining = remaining;
    }

    @Override
    public ArrayList<? extends ParameterInterface> getParameterList() {
        return parameterList;
    }

    @Override
    public ResultWithGeneratedKeys update(Object generatedKeysRequest) {
        ResultWithGeneratedKeys result = commandContainer.executeUpdate(null);
        executeRemaining();
        return result;
    }

    @Override
    public ResultInterface query(long maxrows) {
        ResultInterface result = commandContainer.query(maxrows);
        executeRemaining();
        return result;
    }

    private void executeRemaining() {
        for (Prepared prepared : preparedList) {
            prepared.prepare();

            if (prepared.isQuery()) {
                prepared.query(0);
            } else {
                prepared.update();
            }
        }

        if (remaining != null) {
            remainingCommand = sessionLocal.prepareLocal(remaining);
            remaining = null;
            if (remainingCommand.isQuery()) {
                remainingCommand.query(0);
            } else {
                remainingCommand.update(null);
            }
        }
    }

    @Override
    public void stop() {
        commandContainer.stop();
        for (Prepared prepared : preparedList) {
            CommandContainer.clearCTE(sessionLocal, prepared);
        }
        if (remainingCommand != null) {
            remainingCommand.stop();
        }
    }

    @Override
    public boolean isQuery() {
        return commandContainer.isQuery();
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public ResultInterface queryMeta() {
        return commandContainer.queryMeta();
    }

    @Override
    public int getCommandType() {
        return commandContainer.getCommandType();
    }

    @Override
    public Set<DbObject> getDependencies() {
        HashSet<DbObject> dependencies = new HashSet<>();
        for (Prepared prepared : preparedList) {
            prepared.collectDependencies(dependencies);
        }
        return dependencies;
    }

    @Override
    protected boolean isCurrentCommandADefineCommand() {
        return commandContainer.isCurrentCommandADefineCommand();
    }
}
