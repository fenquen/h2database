/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.dml.SetTypes;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.security.auth.AuthenticationException;
import org.h2.security.auth.AuthenticationInfo;
import org.h2.security.auth.Authenticator;
import org.h2.store.fs.FileUtils;
import org.h2.util.DateTimeUtils;
import org.h2.util.MathUtils;
import org.h2.util.ParserUtil;
import org.h2.util.StringUtils;
import org.h2.util.ThreadDeadlockDetector;
import org.h2.util.TimeZoneProvider;
import org.h2.util.Utils;

import static org.h2.api.ErrorCode.FILE_VERSION_ERROR_1;

/**
 * The engine contains a map of all open databases.
 * It is also responsible for opening and creating new databases.
 * This is a singleton class.
 */
public final class Engine {
    private static final Map<String, DatabaseHolder> DATABASE_PATH_DATABASE_HOLDER = new HashMap<>();
    private static volatile long WRONG_PASSWORD_DELAY = SysProperties.DELAY_WRONG_PASSWORD_MIN;

    private static boolean JMX;

    static {
        // todo rust略过
        if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
            ThreadDeadlockDetector.init();
        }
    }

    private static void throwNotFound(boolean ifExists, boolean forbidCreation, String name) {
        if (ifExists) {
            throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_WITH_IF_EXISTS_1, name);
        }

        if (forbidCreation) {
            throw DbException.get(ErrorCode.REMOTE_DATABASE_NOT_FOUND_1, name);
        }
    }

    /**
     * open a database connection with the given connection information.
     */
    public static SessionLocal createSession(ConnectionInfo connectionInfo) {
        try {
            SessionLocal sessionLocal = openSession(connectionInfo);
            validateUserAndPassword(true);
            return sessionLocal;
        } catch (DbException e) {
            if (e.getErrorCode() == ErrorCode.WRONG_USER_OR_PASSWORD) {
                validateUserAndPassword(false);
            }
            throw e;
        }
    }

    private static SessionLocal openSession(ConnectionInfo connectionInfo) {
        boolean ifExists = connectionInfo.removeProperty("IFEXISTS", false);
        boolean forbidCreation = connectionInfo.removeProperty("FORBID_CREATION", false);
        boolean ignoreUnknownSetting = connectionInfo.removeProperty("IGNORE_UNKNOWN_SETTINGS", false);
        String cipher = connectionInfo.removeProperty("CIPHER", null);
        String init = connectionInfo.removeProperty("INIT", null);

        SessionLocal sessionLocal;

        long start = System.nanoTime();

        for (; ; ) {
            sessionLocal = openSession(connectionInfo, ifExists, forbidCreation, cipher);
            if (sessionLocal != null) {
                break;
            }

            // we found a database that is currently closing
            // wait a bit to avoid a busy loop (the method is synchronized)
            if (System.nanoTime() - start > DateTimeUtils.NANOS_PER_MINUTE) {
                throw DbException.get(ErrorCode.DATABASE_ALREADY_OPEN_1, "Waited for database closing longer than 1 minute");
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw DbException.get(ErrorCode.DATABASE_CALLED_AT_SHUTDOWN);
            }
        }

        synchronized (sessionLocal) {
            sessionLocal.setAllowLiterals(true);

            DbSettings defaultSettings = DbSettings.DEFAULT;

            for (String setting : connectionInfo.getKeys()) {
                if (defaultSettings.containsKey(setting)) {
                    // database setting are only used when opening the database
                    continue;
                }

                String value = connectionInfo.getProperty(setting);
                StringBuilder builder = new StringBuilder("SET ").append(setting).append(' ');

                if (!ParserUtil.isSimpleIdentifier(setting, false, false)) {
                    if (!setting.equalsIgnoreCase("TIME ZONE")) {
                        throw DbException.get(ErrorCode.UNSUPPORTED_SETTING_1, setting);
                    }

                    StringUtils.quoteStringSQL(builder, value);
                } else {
                    builder.append(value);
                }

                try {
                    CommandInterface command = sessionLocal.prepareLocal(builder.toString());
                    command.executeUpdate(null);
                } catch (DbException e) {
                    if (e.getErrorCode() == ErrorCode.ADMIN_RIGHTS_REQUIRED) {
                        sessionLocal.getTrace().error(e, "admin rights required; user: \"" +
                                connectionInfo.getUserName() + "\"");
                    } else {
                        sessionLocal.getTrace().error(e, "");
                    }

                    if (!ignoreUnknownSetting) {
                        sessionLocal.close();
                        throw e;
                    }
                }
            }

            TimeZoneProvider timeZone = connectionInfo.getTimeZone();
            if (timeZone != null) {
                sessionLocal.setTimeZone(timeZone);
            }

            if (init != null) {
                try {
                    CommandInterface command = sessionLocal.prepareLocal(init);
                    command.executeUpdate(null);
                } catch (DbException e) {
                    if (!ignoreUnknownSetting) {
                        sessionLocal.close();
                        throw e;
                    }
                }
            }

            sessionLocal.setAllowLiterals(false);
            sessionLocal.commit(true);
        }

        return sessionLocal;
    }

    private static SessionLocal openSession(ConnectionInfo connectionInfo,
                                            boolean ifExists,
                                            boolean forbidCreation,
                                            String cipher) {
        connectionInfo.removeProperty("NO_UPGRADE", false);

        boolean openNew = connectionInfo.getPropertyBoolean("OPEN_NEW", false);
        boolean opened = false;
        User user = null;
        String databasePath = connectionInfo.getDatabasePath();

        DatabaseHolder databaseHolder;

        if (!connectionInfo.unnamedInMemory) {
            synchronized (DATABASE_PATH_DATABASE_HOLDER) {
                databaseHolder = DATABASE_PATH_DATABASE_HOLDER.computeIfAbsent(databasePath, (key) -> new DatabaseHolder());
            }
        } else {
            databaseHolder = new DatabaseHolder();
        }

        Database database;
        synchronized (databaseHolder) {
            database = databaseHolder.database;
            if (database == null || openNew) {
                if (connectionInfo.persistent) {
                    String value = connectionInfo.getProperty("MV_STORE");
                    String fileName = databasePath + Constants.SUFFIX_MV_FILE;

                    if (value == null) {
                        if (!FileUtils.exists(fileName)) {
                            throwNotFound(ifExists, forbidCreation, databasePath);

                            fileName = databasePath + Constants.SUFFIX_OLD_DATABASE_FILE;

                            if (FileUtils.exists(fileName)) {
                                throw DbException.get(FILE_VERSION_ERROR_1, "old database: " + fileName + " - please convert the database to a SQL script and re-create it.");
                            }

                            fileName = null;
                        }
                    } else {
                        if (!FileUtils.exists(fileName)) {
                            throwNotFound(ifExists, forbidCreation, databasePath);
                            fileName = null;
                        }
                    }

                    // 不是null说明对应的文件已然存在
                    if (fileName != null && !FileUtils.canWrite(fileName)) {
                        connectionInfo.setProperty("ACCESS_MODE_DATA", "r");
                    }
                } else {
                    throwNotFound(ifExists, forbidCreation, databasePath);
                }

                database = new Database(connectionInfo, cipher); // 参数connectionInfo只是为了提供信息不是委身
                opened = true;
                boolean found = false;

                for (RightOwner rightOwner : database.getAllUsersAndRoles()) {
                    if (rightOwner instanceof User) {
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    // users is the last thing we add, so if no user is around,
                    // the database is new (or not initialized correctly)
                    user = new User(database, database.allocateObjectId(), connectionInfo.getUserName(), false);
                    user.admin = true;
                    user.setUserPasswordHash(connectionInfo.getUserPasswordHash());
                    database.setMasterUser(user);
                }

                databaseHolder.database = database;
            }
        }

        if (opened) {
            // start the thread when already synchronizing on the database
            // otherwise a deadlock can occur when the writer thread opens a new database (as in recovery testing)
            database.opened();
        }

        if (database.isClosing()) {
            return null;
        }

        if (user == null) {
            if (database.validateFilePasswordHash(cipher, connectionInfo.filePasswordHash)) {

                if (connectionInfo.getProperty("AUTHREALM") == null) {
                    user = database.findUser(connectionInfo.getUserName());
                    if (user != null) {
                        if (!user.validateUserPasswordHash(connectionInfo.getUserPasswordHash())) {
                            user = null;
                        }
                    }
                } else {
                    Authenticator authenticator = database.getAuthenticator();
                    if (authenticator == null) {
                        throw DbException.get(ErrorCode.AUTHENTICATOR_NOT_AVAILABLE, databasePath);
                    }

                    try {
                        AuthenticationInfo authenticationInfo = new AuthenticationInfo(connectionInfo);
                        user = database.getAuthenticator().authenticate(authenticationInfo, database);
                    } catch (AuthenticationException authenticationError) {
                        database.getTrace(Trace.DATABASE).error(authenticationError,
                                "an error occurred during authentication; user: \"" + connectionInfo.getUserName() + "\"");
                    }
                }
            }

            if (opened && (user == null || !user.isAdmin())) {
                // reset - because the user is not an admin, and has no right to listen to exceptions
                database.setDatabaseEventListener(null);
            }
        }

        if (user == null) {
            DbException er = DbException.get(ErrorCode.WRONG_USER_OR_PASSWORD);
            database.getTrace(Trace.DATABASE).error(er, "wrong user or password; user: \"" + connectionInfo.getUserName() + "\"");
            database.removeSession(null);
            throw er;
        }

        // Prevent to set _PASSWORD
        connectionInfo.cleanAuthenticationInfo();

        checkClustering(connectionInfo, database);

        SessionLocal sessionLocal = database.createSession(user, connectionInfo.networkConnectionInfo);
        if (sessionLocal == null) {   // concurrently closing
            return null;
        }

        if (connectionInfo.getPropertyBoolean("OLD_INFORMATION_SCHEMA", false)) {
            sessionLocal.setOldInformationSchema(true);
        }

        // todo rust略过
        if (connectionInfo.getPropertyBoolean("JMX", false)) {
            try {
                Utils.callStaticMethod("org.h2.jmx.DatabaseInfo.registerMBean", connectionInfo, database);
            } catch (Exception e) {
                database.removeSession(sessionLocal);
                throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1, e, "JMX");
            }

            JMX = true;
        }

        return sessionLocal;
    }

    private static void checkClustering(ConnectionInfo ci, Database database) {
        String clusterSession = ci.getPropertyString(SetTypes.CLUSTER, null);
        if (Constants.CLUSTERING_DISABLED.equals(clusterSession)) {
            // in this case, no checking is made
            // (so that a connection can be made to disable/change clustering)
            return;
        }

        String clusterDb = database.getCluster();
        if (!Constants.CLUSTERING_DISABLED.equals(clusterDb)) {
            if (!Constants.CLUSTERING_ENABLED.equals(clusterSession)) {
                if (!Objects.equals(clusterSession, clusterDb)) {
                    throw DbException.get(ErrorCode.CLUSTER_ERROR_DATABASE_RUNS_CLUSTERED_1, clusterDb);
                }
            }
        }
    }

    /**
     * Called after a database has been closed, to remove the object from the
     * list of open databases.
     *
     * @param name the database name
     */
    static void close(String name) {
        if (JMX) {
            try {
                Utils.callStaticMethod("org.h2.jmx.DatabaseInfo.unregisterMBean", name);
            } catch (Exception e) {
                throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1, e, "JMX");
            }
        }
        synchronized (DATABASE_PATH_DATABASE_HOLDER) {
            DATABASE_PATH_DATABASE_HOLDER.remove(name);
        }
    }

    /**
     * This method is called after validating user name and password. If user
     * name and password were correct, the sleep time is reset, otherwise this
     * method waits some time (to make brute force / rainbow table attacks
     * harder) and then throws a 'wrong user or password' exception. The delay
     * is a bit randomized to protect against timing attacks. Also the delay
     * doubles after each unsuccessful logins, to make brute force attacks
     * harder.
     * <p>
     * There is only one exception message both for wrong user and for
     * wrong password, to make it harder to get the list of user names. This
     * method must only be called from one place, so it is not possible from the
     * stack trace to see if the user name was wrong or the password.
     *
     * @param correct if the user name or the password was correct
     * @throws DbException the exception 'wrong user or password'
     */
    private static void validateUserAndPassword(boolean correct) {
        int min = SysProperties.DELAY_WRONG_PASSWORD_MIN;
        if (correct) {
            long delay = WRONG_PASSWORD_DELAY;
            if (delay > min && delay > 0) {
                // the first correct password must be blocked,
                // otherwise parallel attacks are possible
                synchronized (Engine.class) {
                    // delay up to the last delay
                    // an attacker can't know how long it will be
                    delay = MathUtils.secureRandomInt((int) delay);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    WRONG_PASSWORD_DELAY = min;
                }
            }
        } else {
            // this method is not synchronized on the Engine, so that
            // regular successful attempts are not blocked
            synchronized (Engine.class) {
                long delay = WRONG_PASSWORD_DELAY;

                int max = SysProperties.DELAY_WRONG_PASSWORD_MAX;
                if (max <= 0) {
                    max = Integer.MAX_VALUE;
                }

                WRONG_PASSWORD_DELAY += WRONG_PASSWORD_DELAY;
                if (WRONG_PASSWORD_DELAY > max || WRONG_PASSWORD_DELAY < 0) {
                    WRONG_PASSWORD_DELAY = max;
                }

                if (min > 0) {
                    // a bit more to protect against timing attacks
                    delay += Math.abs(MathUtils.secureRandomLong() % 100);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }

                throw DbException.get(ErrorCode.WRONG_USER_OR_PASSWORD);
            }
        }
    }

    private Engine() {
    }

    private static final class DatabaseHolder {
        volatile Database database;
    }
}
