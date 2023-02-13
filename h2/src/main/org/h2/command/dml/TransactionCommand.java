/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;

/**
 * Represents a transactional statement.
 */
public class TransactionCommand extends Prepared {

    private final int type;
    private String savepointName;
    private String transactionName;

    public TransactionCommand(SessionLocal session, int type) {
        super(session);
        this.type = type;
    }

    public void setSavepointName(String name) {
        this.savepointName = name;
    }

    @Override
    public long update() {
        switch (type) {
        case CommandInterface.SET_AUTOCOMMIT_TRUE:
            sessionLocal.setAutoCommit(true);
            break;
        case CommandInterface.SET_AUTOCOMMIT_FALSE:
            sessionLocal.setAutoCommit(false);
            break;
        case CommandInterface.BEGIN:
            sessionLocal.begin();
            break;
        case CommandInterface.COMMIT:
            sessionLocal.commit(false);
            break;
        case CommandInterface.ROLLBACK:
            sessionLocal.rollback();
            break;
        case CommandInterface.CHECKPOINT:
            sessionLocal.getUser().checkAdmin();
            sessionLocal.getDatabase().checkpoint();
            break;
        case CommandInterface.SAVEPOINT:
            sessionLocal.addSavepoint(savepointName);
            break;
        case CommandInterface.ROLLBACK_TO_SAVEPOINT:
            sessionLocal.rollbackToSavepoint(savepointName);
            break;
        case CommandInterface.CHECKPOINT_SYNC:
            sessionLocal.getUser().checkAdmin();
            sessionLocal.getDatabase().sync();
            break;
        case CommandInterface.PREPARE_COMMIT:
            sessionLocal.prepareCommit(transactionName);
            break;
        case CommandInterface.COMMIT_TRANSACTION:
            sessionLocal.getUser().checkAdmin();
            sessionLocal.setPreparedTransaction(transactionName, true);
            break;
        case CommandInterface.ROLLBACK_TRANSACTION:
            sessionLocal.getUser().checkAdmin();
            sessionLocal.setPreparedTransaction(transactionName, false);
            break;
        case CommandInterface.SHUTDOWN:
        case CommandInterface.SHUTDOWN_COMPACT:
        case CommandInterface.SHUTDOWN_DEFRAG:
            sessionLocal.commit(false);
            //$FALL-THROUGH$
        case CommandInterface.SHUTDOWN_IMMEDIATELY: {
            sessionLocal.getUser().checkAdmin();
            // throttle, to allow testing concurrent
            // execution of shutdown and query
            sessionLocal.throttle();
            Database db = sessionLocal.getDatabase();
            if (db.setExclusiveSession(sessionLocal, true)) {
                db.setCompactMode(type);
                // close the database, but don't update the persistent setting
                db.setCloseDelay(0);
                sessionLocal.close();
            }
            break;
        }
        default:
            throw DbException.getInternalError("type=" + type);
        }
        return 0;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    public void setTransactionName(String string) {
        this.transactionName = string;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

}
