/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

/**
 * A position in a cursor.
 * Instance represents a node in the linked list, which traces path
 * from a specific (target) key within a leaf node all the way up to te root
 * (bottom up path).
 */
public final class CursorPos<K, V> {

    /**
     * 对应的page
     */
    public Page<K, V> page;

    /**
     * page上的index
     * Index of the key (within page above) used to go down to a lower level
     * in case of intermediate nodes, or index of the target key for leaf a node.
     * In a later case, it could be negative, if the key is not present.
     */
    public int index;

    /**
     * Next node in the linked list, representing the position within parent level,
     * or null, if we are at the root level already.
     */
    public CursorPos<K, V> parent;


    public CursorPos(Page<K, V> page, int index, CursorPos<K, V> parent) {
        this.page = page;
        this.index = index;
        this.parent = parent;
    }

    /**
     * Searches for a given key and creates a breadcrumb trail through a B-tree
     * rooted at a given Page. Resulting path starts at "insertion point" for a
     * given key and goes back to the root.
     *
     * @param <K>  key type
     * @param <V>  value type
     * @param page root of the tree
     * @param key  the key to search for
     * @return head of the CursorPos chain (insertion point)
     */
    static <K, V> CursorPos<K, V> traverseDown(Page<K, V> page, K key) {
        CursorPos<K, V> cursorPos = null;
        while (!page.isLeaf()) {
            int index = page.binarySearch(key) + 1;
            if (index < 0) {
                index = -index;
            }

            cursorPos = new CursorPos<>(page, index, cursorPos);
            page = page.getChildPage(index);
        }
        return new CursorPos<>(page, page.binarySearch(key), cursorPos);
    }

    /**
     * calculate the memory used by changes that are not yet stored.
     */
    int processRemovalInfo(long version) {
        int unsavedMemory = 0;
        for (CursorPos<K, V> current = this; current != null; current = current.parent) {
            unsavedMemory += current.page.removePage(version);
        }
        return unsavedMemory;
    }

    @Override
    public String toString() {
        return "CursorPos{" + "page=" + page + ", index=" + index + ", parent=" + parent + '}';
    }
}

