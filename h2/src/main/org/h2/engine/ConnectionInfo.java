/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.h2.api.ErrorCode;
import org.h2.command.dml.SetTypes;
import org.h2.message.DbException;
import org.h2.security.SHA256;
import org.h2.store.fs.FileUtils;
import org.h2.store.fs.encrypt.FilePathEncrypt;
import org.h2.store.fs.rec.FilePathRec;
import org.h2.util.IOUtils;
import org.h2.util.NetworkConnectionInfo;
import org.h2.util.SortedProperties;
import org.h2.util.StringUtils;
import org.h2.util.TimeZoneProvider;
import org.h2.util.Utils;

/**
 * Encapsulates the connection settings, including user name and password.
 */
public class ConnectionInfo implements Cloneable {

    private static final HashSet<String> KNOWN_SETTINGS;

    private static final HashSet<String> IGNORED_BY_PARSER;

    private Properties prop = new Properties();
    private String originalURL;
    public String url;
    private String user;

    public byte[] filePasswordHash;
    public byte[] fileEncryptionKey;
    private byte[] userPasswordHash;

    private TimeZoneProvider timeZone;

    // The database name
    private String name;// ./data/record 实际对应的数据文件是 ./data/record.mv.db
    private String nameNormalized; // /users/a/data/record
    public boolean remote;
    private boolean ssl;
    public boolean persistent;
    public boolean unnamedInMemory;

    public NetworkConnectionInfo networkConnectionInfo;

    /**
     * Create a connection info object.
     *
     * @param name the database name (including tags), but without the
     *             "jdbc:h2:" prefix
     */
    public ConnectionInfo(String name) {
        this.name = name;
        this.url = Constants.START_URL + name;
        parseName();
    }

    /**
     * 该函数必然是在客户端执行的
     * Create a connection info object.
     *
     * @param url      the database URL (must start with jdbc:h2:)
     * @param info     the connection properties or {@code null}
     * @param user     the user name or {@code null}
     * @param password the password as {@code String} or {@code char[]}, or {@code null}
     */
    public ConnectionInfo(String url,
                          Properties info,
                          String user,
                          Object password) {
        // todo rust忽略
        url = remapURL(url);

        originalURL = this.url = url;

        if (!url.startsWith(Constants.START_URL)) {
            throw getFormatException();
        }

        if (info != null) {
            readProperties(info);
        }

        if (user != null) {
            prop.put("USER", user);
        }

        if (password != null) {
            prop.put("PASSWORD", password);
        }

        readSettingsFromURL();

        // todo rust略过
        Object timeZoneName = prop.remove("TIME ZONE");
        if (timeZoneName != null) {
            timeZone = TimeZoneProvider.ofId(timeZoneName.toString());
        }

        setUserName(removeProperty("USER", ""));

        name = this.url.substring(Constants.START_URL.length()); // file:/home/a/file_collector_data/record

        parseName();

        convertPasswords();

        // todo rust忽略
        String recoverTest = removeProperty("RECOVER_TEST", null);
        if (recoverTest != null) {
            FilePathRec.register();
            try {
                Utils.callStaticMethod("org.h2.store.RecoverTester.init", recoverTest);
            } catch (Exception e) {
                throw DbException.convert2DbException(e);
            }
            name = "rec:" + name;
        }
    }

    static {
        String[] commonSettings = { //
                "ACCESS_MODE_DATA", "AUTO_RECONNECT", "AUTO_SERVER", "AUTO_SERVER_PORT", //
                "CACHE_TYPE", //
                "FILE_LOCK", //
                "JMX", //
                "NETWORK_TIMEOUT", //
                "OLD_INFORMATION_SCHEMA", "OPEN_NEW", //
                "PAGE_SIZE", //
                "RECOVER", //
        };

        String[] settings = { //
                "AUTHREALM", "AUTHZPWD", "AUTOCOMMIT", //
                "CIPHER", "CREATE", //
                "FORBID_CREATION", //
                "IGNORE_UNKNOWN_SETTINGS", "IFEXISTS", "INIT", //
                "NO_UPGRADE", //
                "PASSWORD", "PASSWORD_HASH", //
                "RECOVER_TEST", //
                "USER" //
        };

        KNOWN_SETTINGS = new HashSet<>(128);
        KNOWN_SETTINGS.addAll(SetTypes.getTypes());
        for (String setting : commonSettings) {
            if (KNOWN_SETTINGS.add(setting) == false) {
                throw DbException.getInternalError(setting);
            }
        }
        for (String setting : settings) {
            if (KNOWN_SETTINGS.add(setting) == false) {
                throw DbException.getInternalError(setting);
            }
        }

        IGNORED_BY_PARSER = new HashSet<>(64);
        Collections.addAll(IGNORED_BY_PARSER, commonSettings);
        Collections.addAll(IGNORED_BY_PARSER, "ASSERT", "BINARY_COLLATION", "DB_CLOSE_ON_EXIT", "PAGE_STORE", "UUID_COLLATION");
    }

    private static boolean isKnownSetting(String s) {
        return KNOWN_SETTINGS.contains(s);
    }

    /**
     * Returns whether setting with the specified name should be ignored by
     * parser.
     *
     * @param name the name of the setting
     * @return whether setting with the specified name should be ignored by parser
     */
    public static boolean isIgnoredByParser(String name) {
        return IGNORED_BY_PARSER.contains(name);
    }

    @Override
    public ConnectionInfo clone() throws CloneNotSupportedException {
        ConnectionInfo clone = (ConnectionInfo) super.clone();
        clone.prop = (Properties) prop.clone();
        clone.filePasswordHash = Utils.cloneByteArray(filePasswordHash);
        clone.fileEncryptionKey = Utils.cloneByteArray(fileEncryptionKey);
        clone.userPasswordHash = Utils.cloneByteArray(userPasswordHash);
        return clone;
    }

    private void parseName() {
        if (".".equals(name)) {
            name = "mem:";
        }

        if (name.startsWith("tcp:")) {
            remote = true;
            name = name.substring("tcp:".length());
        } else if (name.startsWith("ssl:")) {
            remote = true;
            ssl = true;
            name = name.substring("ssl:".length());
        } else if (name.startsWith("mem:")) {
            persistent = false;
            if ("mem:".equals(name)) {
                unnamedInMemory = true;
            }
        } else if (name.startsWith("file:")) {
            name = name.substring("file:".length());
            persistent = true;
        } else {
            persistent = true;
        }

        if (persistent && !remote) {
            name = IOUtils.nameSeparatorsToNative(name);
        }
    }

    /**
     * Set the base directory of persistent databases, unless the database is incthe user home folder (~).
     */
    public void setBaseDir(String dirPath) {

        if (!persistent) { // 说明要是本地嵌入式模式
            return;
        }

        String absDir = FileUtils.unwrap(FileUtils.toRealPath(dirPath));
        if (dirPath.endsWith(File.separator)) {
            dirPath = dirPath.substring(0, dirPath.length() - 1);
        }

        boolean absolute = FileUtils.isAbsolute(name);

        String n;
        String prefix = null;
        if (absolute) {
            n = name;
        } else {
            n = FileUtils.unwrap(name);
            prefix = name.substring(0, name.length() - n.length());
            n = dirPath + File.separatorChar + n;
        }

        String normalizedName = FileUtils.unwrap(FileUtils.toRealPath(n));
        if (normalizedName.equals(absDir) || !normalizedName.startsWith(absDir)) {
            // database name matches the baseDir or
            // database name is clearly outside the baseDir
            throw DbException.get(ErrorCode.IO_EXCEPTION_1, normalizedName + " outside " + absDir);
        }

        if (absDir.endsWith("/") || absDir.endsWith("\\")) {
            // no further checks are needed for C:/ and similar
        } else if (normalizedName.charAt(absDir.length()) != '/') {
            // database must be within the directory
            // (with baseDir=/test, the database name must not be /test2/x and not /test2)
            throw DbException.get(ErrorCode.IO_EXCEPTION_1, normalizedName + " outside " + absDir);
        }

        if (!absolute) {
            name = prefix + dirPath + File.separatorChar + FileUtils.unwrap(name);
        }
    }

    private void readProperties(Properties info) {
        Object[] ketArr = info.keySet().toArray();
        DbSettings dbSettings = null;

        for (Object key : ketArr) {
            // todo rust忽略 toUpperEnglish内部用到了技巧 暂时略过 使用string自带的uppercase替换
            String uppercaseKey = StringUtils.toUpperEnglish(key.toString());

            if (prop.containsKey(uppercaseKey)) {
                throw DbException.get(ErrorCode.DUPLICATE_PROPERTY_1, uppercaseKey);
            }

            Object value = info.get(key);

            if (isKnownSetting(uppercaseKey)) {
                prop.put(uppercaseKey, value);
                continue;
            }

            // todo rust中可以使用once
            if (dbSettings == null) {
                dbSettings = getDbSettings();
            }

            if (dbSettings.containsKey(uppercaseKey)) {
                prop.put(uppercaseKey, value);
            }
        }
    }

    private void readSettingsFromURL() {
        int idx = url.indexOf(';');
        if (idx < 0) {
            return;
        }

        String settings = url.substring(idx + 1);
        url = url.substring(0, idx);

        String unknownSetting = null;
        String[] list = StringUtils.arraySplit(settings, ';', false);
        for (String setting : list) {
            if (setting.isEmpty()) {
                continue;
            }

            int equal = setting.indexOf('=');
            if (equal < 0) {
                throw getFormatException();
            }

            String key = StringUtils.toUpperEnglish(setting.substring(0, equal));
            String value = setting.substring(equal + 1);

            if (isKnownSetting(key) || DbSettings.DEFAULT.containsKey(key)) {
                String old = prop.getProperty(key);
                if (old != null && !old.equals(value)) {
                    throw DbException.get(ErrorCode.DUPLICATE_PROPERTY_1, key);
                }

                prop.setProperty(key, value);
            } else {
                unknownSetting = key;
            }
        }

        if (unknownSetting != null && !Utils.parseBoolean(prop.getProperty("IGNORE_UNKNOWN_SETTINGS"), false, false)) {
            throw DbException.get(ErrorCode.UNSUPPORTED_SETTING_1, unknownSetting);
        }
    }

    private char[] removePassword() {
        Object password = prop.remove("PASSWORD");

        if ((!remote || ssl) && prop.containsKey("AUTHREALM") && password != null) {
            prop.put("AUTHZPWD", password instanceof char[] ? new String((char[]) password) : password);
        }

        if (password == null) {
            return new char[0];
        }

        if (password instanceof char[]) {
            return (char[]) password;
        }

        return password.toString().toCharArray();
    }

    /**
     * Split the password property into file password and user password if
     * necessary, and convert them to the internal hash format.
     */
    private void convertPasswords() {
        char[] password = removePassword();
        boolean passwordHash = removeProperty("PASSWORD_HASH", false);

        // todo rust略过
        if (getPropertyString("CIPHER", null) != null) {
            // split password into (filePassword+' '+userPassword)
            int space = -1;
            for (int i = 0, len = password.length; i < len; i++) {
                if (password[i] == ' ') {
                    space = i;
                    break;
                }
            }
            if (space < 0) {
                throw DbException.get(ErrorCode.WRONG_PASSWORD_FORMAT);
            }

            char[] np = Arrays.copyOfRange(password, space + 1, password.length);
            char[] filePassword = Arrays.copyOf(password, space);
            Arrays.fill(password, (char) 0);
            password = np;
            fileEncryptionKey = FilePathEncrypt.getPasswordBytes(filePassword);
            filePasswordHash = hashPassword(passwordHash, "file", filePassword);
        }

        userPasswordHash = hashPassword(passwordHash, user, password);
    }

    private static byte[] hashPassword(boolean passwordHash, String userName, char[] password) {
        if (passwordHash) {
            return StringUtils.convertHexString2ByteArr(new String(password));
        }

        // todo rust略过以下
        if (userName.isEmpty() && password.length == 0) {
            return new byte[0];
        }

        return SHA256.getKeyPasswordHash(userName, password);
    }

    /**
     * Get a boolean property if it is set and return the value.
     *
     * @param key          the property name
     * @param defaultValue the default value
     * @return the value
     */
    public boolean getPropertyBoolean(String key, boolean defaultValue) {
        return Utils.parseBoolean(getPropertyString(key, null), defaultValue, false);
    }

    /**
     * Remove a boolean property if it is set and return the value.
     *
     * @param key          the property name
     * @param defaultValue the default value
     * @return the value
     */
    public boolean removeProperty(String key, boolean defaultValue) {
        return Utils.parseBoolean(removeProperty(key, null), defaultValue, false);
    }

    /**
     * Remove a String property if it is set and return the value.
     *
     * @param key          the property name
     * @param defaultValue the default value
     * @return the value
     */
    String removeProperty(String key, String defaultValue) {
        // todo rust忽略
        if (SysProperties.CHECK && !isKnownSetting(key)) {
            throw DbException.getInternalError(key);
        }

        Object x = prop.remove(key);
        return x == null ? defaultValue : x.toString();
    }

    /**
     * Get the unique and normalized database name (excluding settings).
     *
     * @return the database name
     */
    public String getDatabasePath() {
        if (!persistent) {
            return name;
        }

        if (nameNormalized == null) {
            if (!FileUtils.isAbsolute(name) &&
                    !name.contains("./") &&
                    !name.contains(".\\") &&
                    !name.contains(":/") &&
                    !name.contains(":\\")) {
                // the name could start with "./", or
                // it could start with a prefix such as "nioMapped:./"
                // for Windows, the path "\test" is not considered
                // absolute as the drive letter is missing,
                // but we consider it absolute
                throw DbException.get(ErrorCode.URL_RELATIVE_TO_CWD, originalURL);
            }

            String real = FileUtils.toRealPath(name + Constants.SUFFIX_MV_FILE);
            String fileName = FileUtils.getName(real);

            if (fileName.length() < Constants.SUFFIX_MV_FILE.length() + 1) {
                throw DbException.get(ErrorCode.INVALID_DATABASE_NAME_1, name);
            }

            nameNormalized = real.substring(0, real.length() - Constants.SUFFIX_MV_FILE.length());
        }

        return nameNormalized;
    }

    byte[] getFileEncryptionKey() {
        return fileEncryptionKey;
    }

    /**
     * Get the name of the user.
     *
     * @return the user name
     */
    public String getUserName() {
        return user;
    }

    /**
     * Get the user password hash.
     *
     * @return the password hash
     */
    byte[] getUserPasswordHash() {
        return userPasswordHash;
    }

    /**
     * Get the property keys.
     *
     * @return the property keys
     */
    String[] getKeys() {
        return prop.keySet().toArray(new String[prop.size()]);
    }

    /**
     * Get the value of the given property.
     *
     * @param key the property key
     * @return the value as a String
     */
    String getProperty(String key) {
        Object value = prop.get(key);
        if (!(value instanceof String)) {
            return null;
        }
        return value.toString();
    }

    /**
     * Get the value of the given property.
     *
     * @param key          the property key
     * @param defaultValue the default value
     * @return the value as a String
     */
    int getPropertyInt(String key, int defaultValue) {
        if (SysProperties.CHECK && !isKnownSetting(key)) {
            throw DbException.getInternalError(key);
        }

        String s = getProperty(key);
        return s == null ? defaultValue : Integer.parseInt(s);
    }

    /**
     * Get the value of the given property.
     *
     * @param key          the property key
     * @param defaultValue the default value
     * @return the value as a String
     */
    public String getPropertyString(String key, String defaultValue) {
        if (SysProperties.CHECK && !isKnownSetting(key)) {
            throw DbException.getInternalError(key);
        }
        String s = getProperty(key);
        return s == null ? defaultValue : s;
    }

    /**
     * Get the value of the given property.
     *
     * @param setting      the setting id
     * @param defaultValue the default value
     * @return the value as a String
     */
    String getPropertyString(int setting, String defaultValue) {
        String key = SetTypes.getTypeName(setting);
        String s = getProperty(key);
        return s == null ? defaultValue : s;
    }

    /**
     * Get the value of the given property.
     *
     * @param setting      the setting id
     * @param defaultValue the default value
     * @return the value as an integer
     */
    int getIntProperty(int setting, int defaultValue) {
        String key = SetTypes.getTypeName(setting);
        String s = getPropertyString(key, null);
        try {
            return s == null ? defaultValue : Integer.decode(s);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Check if this is a remote connection with SSL enabled.
     *
     * @return true if it is
     */
    boolean isSSL() {
        return ssl;
    }

    /**
     * Overwrite the user name. The user name is case-insensitive and stored in
     * uppercase. English conversion is used.
     *
     * @param name the user name
     */
    public void setUserName(String name) {
        this.user = StringUtils.toUpperEnglish(name);
    }

    /**
     * Set the user password hash.
     *
     * @param hash the new hash value
     */
    public void setUserPasswordHash(byte[] hash) {
        this.userPasswordHash = hash;
    }

    /**
     * Set the file password hash.
     *
     * @param hash the new hash value
     */
    public void setFilePasswordHash(byte[] hash) {
        this.filePasswordHash = hash;
    }

    public void setFileEncryptionKey(byte[] key) {
        this.fileEncryptionKey = key;
    }

    public void setProperty(String key, String value) {
        if (value != null) {
            prop.setProperty(key, value);
        }
    }

    /**
     * Get the complete original database URL.
     *
     * @return the database URL
     */
    public String getOriginalURL() {
        return originalURL;
    }

    /**
     * Set the original database URL.
     *
     * @param url the database url
     */
    public void setOriginalURL(String url) {
        originalURL = url;
    }

    /**
     * Returns the time zone.
     *
     * @return the time zone
     */
    public TimeZoneProvider getTimeZone() {
        return timeZone;
    }

    /**
     * Generate a URL format exception.
     *
     * @return the exception
     */
    DbException getFormatException() {
        return DbException.get(ErrorCode.URL_FORMAT_ERROR_2, Constants.URL_FORMAT, url);
    }

    /**
     * Switch to server mode, and set the server name and database key.
     *
     * @param serverKey the server name, '/', and the security key
     */
    public void setServerKey(String serverKey) {
        remote = true;
        persistent = false;
        this.name = serverKey;
    }

    /**
     * Returns the network connection information, or {@code null}.
     *
     * @return the network connection information, or {@code null}
     */
    public NetworkConnectionInfo getNetworkConnectionInfo() {
        return networkConnectionInfo;
    }

    /**
     * Sets the network connection information.
     *
     * @param networkConnectionInfo the network connection information
     */
    public void setNetworkConnectionInfo(NetworkConnectionInfo networkConnectionInfo) {
        this.networkConnectionInfo = networkConnectionInfo;
    }

    public DbSettings getDbSettings() {
        HashMap<String, String> settings = new HashMap<>(DbSettings.TABLE_SIZE);
        for (Object k : prop.keySet()) {
            String key = k.toString();
            if (!isKnownSetting(key) && DbSettings.DEFAULT.containsKey(key)) {
                settings.put(key, prop.getProperty(key));
            }
        }
        return new DbSettings(settings);
    }

    private static String remapURL(String url) {
        String urlMap = SysProperties.URL_MAP;
        if (urlMap != null && !urlMap.isEmpty()) {
            try {
                SortedProperties prop;
                prop = SortedProperties.loadProperties(urlMap);
                String url2 = prop.getProperty(url);
                if (url2 == null) {
                    prop.put(url, "");
                    prop.store(urlMap);
                } else {
                    url2 = url2.trim();
                    if (!url2.isEmpty()) {
                        return url2;
                    }
                }
            } catch (IOException e) {
                throw DbException.convert2DbException(e);
            }
        }
        return url;
    }

    /**
     * Clear authentication properties.
     */
    public void cleanAuthenticationInfo() {
        removeProperty("AUTHREALM", false);
        removeProperty("AUTHZPWD", false);
    }
}
