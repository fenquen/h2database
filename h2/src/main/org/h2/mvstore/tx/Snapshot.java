/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import java.util.BitSet;

import org.h2.mvstore.RootReference;

public final class Snapshot<K, V> {

    /**
     * transactionMap 的 mvMap 的 rootReference
     */
    final RootReference<K, V> rootReference;

    /**
     * The committing transactions (see also TransactionStore.committingTransactions).
     */
    final BitSet committingTxs;

    Snapshot(RootReference<K, V> rootReference, BitSet committingTxs) {
        this.rootReference = rootReference;
        this.committingTxs = committingTxs;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + committingTxs.hashCode();
        result = prime * result + rootReference.hashCode();
        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Snapshot)) {
            return false;
        }
        Snapshot<K, V> other = (Snapshot<K, V>) obj;
        return committingTxs == other.committingTxs && rootReference == other.rootReference;
    }

}
