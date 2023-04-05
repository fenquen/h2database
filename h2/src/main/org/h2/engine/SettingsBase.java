/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.Utils;

public class SettingsBase {

    protected final HashMap<String, String> settings;

    protected SettingsBase(HashMap<String, String> settings) {
        this.settings = settings;
    }

    protected boolean get(String key, boolean defaultValue) {
        String s = get(key, Boolean.toString(defaultValue));
        try {
            return Utils.parseBoolean(s, defaultValue, true);
        } catch (IllegalArgumentException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, "key:" + key + " value:" + s);
        }
    }

    protected int get(String key, int defaultValue) {
        String s = get(key, Integer.toString(defaultValue));
        try {
            return Integer.decode(s);
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, "key:" + key + " value:" + s);
        }
    }

    protected String get(String key, String defaultValue) {
        String setting = settings.get(key);
        if (setting != null) {
            return setting;
        }

        // todo rust略过 以下的拼字符串是为了读取system property 现在暂时不去实现其实可以通过命令行参数-Dname
        StringBuilder stringBuilder = new StringBuilder("h2.");
        boolean nextUpper = false;
        for (int i = 0, l = key.length(); i < l; i++) {
            char c = key.charAt(i);
            if (c == '_') {
                nextUpper = true;
            } else {
                // Character.toUpperCase / toLowerCase ignores the locale
                stringBuilder.append(nextUpper ? Character.toUpperCase(c) : Character.toLowerCase(c));
                nextUpper = false;
            }
        }
        String systemPropertyName = stringBuilder.toString();

        setting = Utils.getProperty(systemPropertyName, defaultValue);
        settings.put(key, setting);

        return setting;
    }

    protected boolean containsKey(String k) {
        return settings.containsKey(k);
    }

    public HashMap<String, String> getSettings() {
        return settings;
    }

    /**
     * Get all settings in alphabetical order.
     */
    public Entry<String, String>[] getSortedSettings() {
        @SuppressWarnings("unchecked")
        Map.Entry<String, String>[] entries = settings.entrySet().toArray(new Map.Entry[0]);
        Arrays.sort(entries, Entry.comparingByKey());
        return entries;
    }
}