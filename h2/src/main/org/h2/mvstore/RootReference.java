/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

/**
 * represent state of the MVMap as a whole
 * (not related to a particular B-Tree node).
 * Single structure would allow for non-blocking atomic state change.
 * The most important part of it is a reference to the root node.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class RootReference<K, V> {

    /**
     * The root page.
     */
    public final Page<K, V> rootPage;

    /**
     * The version used for writing.
     */
    public final long version;
    
    /**
     * Counter of reentrant locks.
     */
    private final byte holdCount;

    /**
     * Lock owner thread id.
     */
    private final long ownerId;
    /**
     * Reference to the previous root in the chain.
     * That is the last root of the previous version, which had any data changes.
     * Versions without any data changes are dropped from the chain, as it built.
     */
    volatile RootReference<K, V> previous;
    /**
     * Counter for successful root updates.
     */
    final long updateCounter;

    /**
     * Counter for attempted root updates.
     */
    final long updateAttemptCounter;

    /**
     * Size of the occupied part of the append buffer.
     */
    private final byte appendCounter;


    // This one is used to set root initially and for r/o snapshots
    RootReference(Page<K, V> rootPage, long version) {
        this.rootPage = rootPage;
        this.version = version;
        this.holdCount = 0;
        this.ownerId = 0;
        this.previous = null;
        this.updateCounter = 1;
        this.updateAttemptCounter = 1;
        this.appendCounter = 0;
    }

    private RootReference(RootReference<K, V> rootReference,
                          Page<K, V> rootPage,
                          long updateAttemptCounter) {
        this.rootPage = rootPage;
        this.version = rootReference.version;
        this.previous = rootReference.previous;
        this.updateCounter = rootReference.updateCounter + 1;
        this.updateAttemptCounter = rootReference.updateAttemptCounter + updateAttemptCounter;
        this.holdCount = 0;
        this.ownerId = 0;
        this.appendCounter = rootReference.appendCounter;
    }

    // This one is used for locking
    private RootReference(RootReference<K, V> r, int attempt) {
        this.rootPage = r.rootPage;
        this.version = r.version;
        this.previous = r.previous;
        this.updateCounter = r.updateCounter + 1;
        this.updateAttemptCounter = r.updateAttemptCounter + attempt;
        assert r.holdCount == 0 || r.ownerId == Thread.currentThread().getId() : Thread.currentThread().getId() + " " + r;
        this.holdCount = (byte) (r.holdCount + 1);
        this.ownerId = Thread.currentThread().getId();
        this.appendCounter = r.appendCounter;
    }

    // This one is used for unlocking
    private RootReference(RootReference<K, V> r, Page<K, V> rootPage, boolean keepLocked, int appendCounter) {
        this.rootPage = rootPage;
        this.version = r.version;
        this.previous = r.previous;
        this.updateCounter = r.updateCounter;
        this.updateAttemptCounter = r.updateAttemptCounter;
        assert r.holdCount > 0 && r.ownerId == Thread.currentThread().getId() : Thread.currentThread().getId() + " " + r;
        this.holdCount = (byte) (r.holdCount - (keepLocked ? 0 : 1));
        this.ownerId = this.holdCount == 0 ? 0 : Thread.currentThread().getId();
        this.appendCounter = (byte) appendCounter;
    }

    // used for version change
    private RootReference(RootReference<K, V> rootReference, long version, int attempt) {
        RootReference<K, V> previous = rootReference;

        RootReference<K, V> tmp;
        while ((tmp = previous.previous) != null && tmp.rootPage == rootReference.rootPage) {
            previous = tmp;
        }

        this.rootPage = rootReference.rootPage;
        this.version = version;
        this.previous = previous;
        this.updateCounter = rootReference.updateCounter + 1;
        this.updateAttemptCounter = rootReference.updateAttemptCounter + attempt;
        this.holdCount = rootReference.holdCount == 0 ? 0 : (byte) (rootReference.holdCount - 1);
        this.ownerId = this.holdCount == 0 ? 0 : rootReference.ownerId;

        assert rootReference.appendCounter == 0;
        this.appendCounter = 0;
    }

    /**
     * Try to unlock.
     *
     * @param newRootPage    the new root page
     * @param attemptCounter the number of attempts so far
     * @return the new, unlocked, root reference, or null if not successful
     */
    RootReference<K, V> updateRootPage(Page<K, V> newRootPage, long attemptCounter) {
        return isFree() ? tryUpdate(new RootReference<>(this, newRootPage, attemptCounter)) : null;
    }

    /**
     * @param attemptCounter the number of attempts so far
     * @return the new, locked, root reference, or null if not successful
     */
    RootReference<K, V> tryLock(int attemptCounter) {
        return canUpdate() ? tryUpdate(new RootReference<>(this, attemptCounter)) : null;
    }

    /**
     * Try to unlock, and if successful update the version
     *
     * @param version the version
     * @param attempt the number of attempts so far
     * @return the new, unlocked and updated, root reference, or null if not successful
     */
    RootReference<K, V> tryUnlockAndUpdateVersion(long version, int attempt) {
        return canUpdate() ? tryUpdate(new RootReference<>(this, version, attempt)) : null;
    }

    /**
     * Update the page, possibly keeping it locked.
     *
     * @param page          the page
     * @param keepLocked    whether to keep it locked
     * @param appendCounter number of items in append buffer
     * @return the new root reference, or null if not successful
     */
    RootReference<K, V> updatePageAndLockedStatus(Page<K, V> page, boolean keepLocked, int appendCounter) {
        return canUpdate() ? tryUpdate(new RootReference<>(this, page, keepLocked, appendCounter)) : null;
    }

    /**
     * removed old versions that are no longer used.
     */
    void removeUnusedOldVersions(long oldestVersionToKeep) {
        // We need to keep at least one previous version (if any) here,
        // because in order to retain whole history of some version
        // we really need last root of the previous version.
        // Root labeled with version "X" is the LAST known root for that version
        // and therefore the FIRST known root for the version "X+1"
        for (RootReference<K, V> rootReference = this; rootReference != null; rootReference = rootReference.previous) {
            if (rootReference.version < oldestVersionToKeep) {
                RootReference<K, V> previous;
                assert (previous = rootReference.previous) == null || previous.getAppendCounter() == 0 : oldestVersionToKeep + " " + rootReference.previous;
                // 只要执行了那么链条便断了 for循环停止
                rootReference.previous = null;
            }
        }
    }

    boolean isLocked() {
        return holdCount != 0;
    }

    private boolean isFree() {
        return holdCount == 0;
    }

    private boolean canUpdate() {
        return isFree() || ownerId == Thread.currentThread().getId();
    }

    public boolean isLockedByCurrentThread() {
        return holdCount != 0 && ownerId == Thread.currentThread().getId();
    }

    private RootReference<K, V> tryUpdate(RootReference<K, V> updatedRootReference) {
        assert canUpdate();
        return rootPage.mvMap.compareAndSetRoot(this, updatedRootReference) ? updatedRootReference : null;
    }

    long getVersion() {
        RootReference<K, V> prev = previous;
        return prev == null || prev.rootPage != rootPage || prev.appendCounter != appendCounter ? version : prev.getVersion();
    }

    /**
     * Does the root have changes since the specified version?
     *
     * @param version    to check against
     * @param persistent whether map is backed by persistent storage
     * @return true if this root has unsaved changes
     */
    boolean hasChangesSince(long version, boolean persistent) {
        return persistent && (rootPage.isSaved() ? getAppendCounter() > 0 : getTotalCount() > 0) || getVersion() > version;
    }

    int getAppendCounter() {
        return appendCounter & 0xff;
    }

    public boolean needFlush() {
        return appendCounter != 0;
    }

    public long getTotalCount() {
        return rootPage.getTotalCount() + getAppendCounter();
    }

    @Override
    public String toString() {
        return "RootReference(" + System.identityHashCode(rootPage) +
                ", v=" + version +
                ", owner=" + ownerId + (ownerId == Thread.currentThread().getId() ? "(current)" : "") +
                ", holdCnt=" + holdCount +
                ", keys=" + rootPage.getTotalCount() +
                ", append=" + getAppendCounter() +
                ")";
    }
}
