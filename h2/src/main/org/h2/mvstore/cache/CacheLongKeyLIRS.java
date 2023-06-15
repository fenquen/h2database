/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.cache;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.h2.mvstore.DataUtils;

/**
 * A scan resistant cache that uses keys of type long. It is meant to cache
 * objects that are relatively costly to acquire, for example file content.
 * <p>
 * This implementation is multi-threading safe and supports concurrent access.
 * Null keys or null values are not allowed. The map fill factor is at most 75%.
 * <p>
 * Each entry is assigned a distinct memory size, and the cache will try to use
 * at most the specified amount of memory. The memory unit is not relevant,
 * however it is suggested to use bytes as the unit.
 * <p>
 * This class implements an approximation of the LIRS replacement algorithm
 * invented by Xiaodong Zhang and Song Jiang as described in
 * https://web.cse.ohio-state.edu/~zhang.574/lirs-sigmetrics-02.html with a few
 * smaller changes: An additional queue for non-resident entries is used, to
 * prevent unbound memory usage. The maximum size of this queue is at most the
 * size of the rest of the stack. About 6.25% of the mapped entries are cold.
 * <p>
 * Internally, the cache is split into a number of segments, and each segment is
 * an individual LIRS cache.
 * <p>
 * Accessed entries are only moved to the top of the stack if at least a number
 * of other entries have been moved to the front (8 per segment by default).
 * Write access and moving entries to the top of the stack is synchronized per
 * segment.
 *
 * @param <V> the value type
 * @author Thomas Mueller
 */
public class CacheLongKeyLIRS<V> {

    /**
     * the maximum memory this cache should use.
     */
    private long maxMemory;

    private final Segment<V>[] segmentArr;

    private final int segmentCount;
    private final int segmentShift;
    private final int segmentMask;
    private final int stackMoveDistance;
    private final int nonResidentQueueSize;
    private final int nonResidentQueueSizeHigh;

    /**
     * Create a new cache with the given memory size.
     */
    @SuppressWarnings("unchecked")
    public CacheLongKeyLIRS(Config config) {
        setMaxMemory(config.maxMemory);
        this.nonResidentQueueSize = config.nonResidentQueueSize;
        this.nonResidentQueueSizeHigh = config.nonResidentQueueSizeHigh;
        DataUtils.checkArgument(Integer.bitCount(config.segmentCount) == 1, "The segment count must be a power of 2, is {0}", config.segmentCount);
        this.segmentCount = config.segmentCount;
        this.segmentMask = segmentCount - 1;
        this.stackMoveDistance = config.stackMoveDistance;
        segmentArr = new Segment[segmentCount];
        clear();
        // use the high bits for the segment
        this.segmentShift = 32 - Integer.bitCount(segmentMask);
    }

    /**
     * Remove all entries.
     */
    public void clear() {
        long max = getMaxItemSize();
        for (int i = 0; i < segmentCount; i++) {
            segmentArr[i] = new Segment<>(max, stackMoveDistance, 8, nonResidentQueueSize, nonResidentQueueSizeHigh);
        }
    }

    /**
     * determines max size of the data item size to fit into cache
     */
    public long getMaxItemSize() {
        return Math.max(1, maxMemory / segmentCount);
    }

    private Entry<V> find(long key) {
        int hash = getHash(key);
        return getSegment(hash).find(key, hash);
    }

    /**
     * Check whether there is a resident entry for the given key. This
     * method does not adjust the internal state of the cache.
     *
     * @param key the key (may not be null)
     * @return true if there is a resident entry
     */
    public boolean containsKey(long key) {
        Entry<V> e = find(key);
        return e != null && e.value != null;
    }

    /**
     * Get the value for the given key if the entry is cached. This method does
     * not modify the internal state.
     *
     * @param key the key (may not be null)
     * @return the value, or null if there is no resident entry
     */
    public V peek(long key) {
        Entry<V> e = find(key);
        return e == null ? null : e.getValue();
    }

    /**
     * Add an entry to the cache using the average memory size.
     *
     * @param key   the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value, or null if there was no resident entry
     */
    public V put(long key, V value) {
        return put(key, value, sizeOf(value));
    }

    /**
     * Add an entry to the cache. The entry may or may not exist in the
     * cache yet. This method will usually mark unknown entries as cold and
     * known entries as hot.
     *
     * @param key    the key (may not be null)
     * @param value  the value (may not be null)
     * @param memory the memory used for the given entry
     * @return the old value, or null if there was no resident entry
     */
    public V put(long key, V value, int memory) {
        if (value == null) {
            throw DataUtils.newIllegalArgumentException("The value may not be null");
        }

        int hash = getHash(key);
        int segmentIndex = getSegmentIndex(hash);
        Segment<V> segment = segmentArr[segmentIndex];

        // check whether resize is required: synchronize on s, to avoid
        // concurrent resizes (concurrent reads read from the old segment)
        synchronized (segment) {
            segment = resizeIfNeeded(segment, segmentIndex);
            return segment.put(key, hash, value, memory);
        }
    }

    private Segment<V> resizeIfNeeded(Segment<V> segment, int segmentIndex) {
        int newLen = segment.getNewMapLen();
        if (newLen == 0) {
            return segment;
        }

        // another thread might have resized (as we retrieved the segment before synchronizing on it)
        Segment<V> s2 = segmentArr[segmentIndex];
        if (segment == s2) {
            // no other thread resized, so we do
            segment = new Segment<>(segment, newLen);
            segmentArr[segmentIndex] = segment;
        }

        return segment;
    }

    /**
     * Get the size of the given value. The default implementation returns 1.
     *
     * @param value the value
     * @return the size
     */
    @SuppressWarnings("unused")
    protected int sizeOf(V value) {
        return 1;
    }

    /**
     * Remove an entry. Both resident and non-resident entries can be
     * removed.
     *
     * @param key the key (may not be null)
     * @return the old value, or null if there was no resident entry
     */
    public V remove(long key) {
        int hash = getHash(key);
        int segmentIndex = getSegmentIndex(hash);
        Segment<V> s = segmentArr[segmentIndex];
        // check whether resize is required: synchronize on s, to avoid
        // concurrent resizes (concurrent reads read
        // from the old segment)
        synchronized (s) {
            s = resizeIfNeeded(s, segmentIndex);
            return s.remove(key, hash);
        }
    }

    /**
     * Get the memory used for the given key.
     *
     * @param key the key (may not be null)
     * @return the memory, or 0 if there is no resident entry
     */
    public int getMemory(long key) {
        Entry<V> e = find(key);
        return e == null ? 0 : e.getMemory();
    }

    /**
     * Get the value for the given key if the entry is cached. This method
     * adjusts the internal state of the cache sometimes, to ensure commonly
     * used entries stay in the cache.
     *
     * @param key the key (may not be null)
     * @return the value, or null if there is no resident entry
     */
    public V get(long key) {
        int hash = getHash(key);
        Segment<V> segment = getSegment(hash);
        Entry<V> entry = segment.find(key, hash);
        return segment.get(entry);
    }

    private Segment<V> getSegment(int hash) {
        return segmentArr[getSegmentIndex(hash)];
    }

    private int getSegmentIndex(int hash) {
        return (hash >>> segmentShift) & segmentMask;
    }

    /**
     * Get the hash code for the given key. The hash code is
     * further enhanced to spread the values more evenly.
     *
     * @param key the key
     * @return the hash code
     */
    static int getHash(long key) {
        int hash = (int) ((key >>> 32) ^ key);
        // a supplemental secondary hash function to protect against hash codes that don't differ much
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = (hash >>> 16) ^ hash;
        return hash;
    }

    /**
     * Get the currently used memory.
     *
     * @return the used memory
     */
    public long getUsedMemory() {
        long x = 0;
        for (Segment<V> s : segmentArr) {
            x += s.usedMemory;
        }
        return x;
    }

    /**
     * Set the maximum memory this cache should use. This will not
     * immediately cause entries to get removed however; it will only change
     * the limit. To resize the internal array, call the clear method.
     *
     * @param maxMemory the maximum size (1 or larger) in bytes
     */
    public void setMaxMemory(long maxMemory) {
        DataUtils.checkArgument(maxMemory > 0, "Max memory must be larger than 0, is {0}", maxMemory);
        this.maxMemory = maxMemory;

        if (segmentArr != null) {
            long max = 1 + maxMemory / segmentArr.length;
            for (Segment<V> segment : segmentArr) {
                segment.maxMemory = max;
            }
        }
    }

    public long getMaxMemory() {
        return maxMemory;
    }

    /**
     * Get the entry set for all resident entries.
     *
     * @return the entry set
     */
    public synchronized Set<Map.Entry<Long, V>> entrySet() {
        return getMap().entrySet();
    }

    /**
     * Get the set of keys for resident entries.
     *
     * @return the set of keys
     */
    public Set<Long> keySet() {
        HashSet<Long> set = new HashSet<>();
        for (Segment<V> s : segmentArr) {
            set.addAll(s.keySet());
        }
        return set;
    }

    /**
     * Get the number of non-resident entries in the cache.
     *
     * @return the number of non-resident entries
     */
    public int sizeNonResident() {
        int x = 0;
        for (Segment<V> s : segmentArr) {
            x += s.queue2Size;
        }
        return x;
    }

    /**
     * Get the length of the internal map array.
     *
     * @return the size of the array
     */
    public int sizeMapArray() {
        int x = 0;
        for (Segment<V> s : segmentArr) {
            x += s.entries.length;
        }
        return x;
    }

    /**
     * Get the number of hot entries in the cache.
     *
     * @return the number of hot entries
     */
    public int sizeHot() {
        int x = 0;
        for (Segment<V> s : segmentArr) {
            x += s.mapSize - s.queueSize - s.queue2Size;
        }
        return x;
    }

    /**
     * Get the number of cache hits.
     *
     * @return the cache hits
     */
    public long getHits() {
        long x = 0;
        for (Segment<V> s : segmentArr) {
            x += s.hitCount;
        }
        return x;
    }

    /**
     * Get the number of cache misses.
     *
     * @return the cache misses
     */
    public long getMisses() {
        int x = 0;
        for (Segment<V> s : segmentArr) {
            x += s.missCount;
        }
        return x;
    }

    /**
     * Get the number of resident entries.
     *
     * @return the number of entries
     */
    public int size() {
        int x = 0;
        for (Segment<V> s : segmentArr) {
            x += s.mapSize - s.queue2Size;
        }
        return x;
    }

    /**
     * Get the list of keys. This method allows to read the internal state of
     * the cache.
     *
     * @param cold        if true, only keys for the cold entries are returned
     * @param nonResident true for non-resident entries
     * @return the key list
     */
    public List<Long> keys(boolean cold, boolean nonResident) {
        ArrayList<Long> keys = new ArrayList<>();
        for (Segment<V> s : segmentArr) {
            keys.addAll(s.keys(cold, nonResident));
        }
        return keys;
    }

    /**
     * Get the values for all resident entries.
     *
     * @return the entry set
     */
    public List<V> values() {
        ArrayList<V> list = new ArrayList<>();
        for (long k : keySet()) {
            V value = peek(k);
            if (value != null) {
                list.add(value);
            }
        }
        return list;
    }

    /**
     * Check whether the cache is empty.
     *
     * @return true if it is empty
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Check whether the given value is stored.
     *
     * @param value the value
     * @return true if it is stored
     */
    public boolean containsValue(V value) {
        return getMap().containsValue(value);
    }

    /**
     * Convert this cache to a map.
     *
     * @return the map
     */
    public Map<Long, V> getMap() {
        HashMap<Long, V> map = new HashMap<>();
        for (long k : keySet()) {
            V x = peek(k);
            if (x != null) {
                map.put(k, x);
            }
        }
        return map;
    }

    /**
     * Add all elements of the map to this cache.
     *
     * @param m the map
     */
    public void putAll(Map<Long, ? extends V> m) {
        for (Map.Entry<Long, ? extends V> e : m.entrySet()) {
            // copy only non-null entries
            put(e.getKey(), e.getValue());
        }
    }

    /**
     * Loop through segments, trimming the non resident queue.
     */
    public void trimNonResidentQueue() {
        for (Segment<V> s : segmentArr) {
            synchronized (s) {
                s.trimNonResidentQueue();
            }
        }
    }

    private static class Segment<V> {

        /**
         * The number of (hot, cold, and non-resident) entries in the map.
         */
        int mapSize;

        /**
         * The size of the LIRS queue for resident cold entries.
         */
        int queueSize;

        /**
         * The size of the LIRS queue for non-resident cold entries.
         */
        int queue2Size;

        /**
         * The number of cache hits.
         */
        long hitCount;

        /**
         * The number of cache misses.
         */
        long missCount;

        /**
         * The map array. The size is always a power of 2.
         */
        final Entry<V>[] entries;

        /**
         * The currently used memory.
         */
        long usedMemory;

        /**
         * How many other item are to be moved to the top of the stack before the current item is moved.
         */
        private final int stackMoveDistance;

        /**
         * Set the maximum memory this cache should use. This will not
         * immediately cause entries to get removed however; it will only change
         * the limit. To resize the internal array, call the clear method.
         * the maximum size (1 or larger) this cache should use in bytes
         */
        private long maxMemory;

        /**
         * The bit mask that is applied to the key hash code to get the index in
         * the map array. The mask is the length of the array minus one.
         */
        private final int mask;

        /**
         * Low watermark for the number of entries in the non-resident queue,
         * as a factor of the number of entries in the map.
         */
        private final int nonResidentQueueSize;

        /**
         * High watermark for the number of entries in the non-resident queue,
         * as a factor of the number of entries in the map.
         */
        private final int nonResidentQueueSizeHigh;

        /**
         * The stack of recently referenced elements.
         * This includes hot entries , recently referenced cold entries.
         * not include Resident cold entries that were not recently referenced,non-resident cold entries
         * <p>
         * There is always at least one entry: the head entry.
         */
        private final Entry<V> stack;

        /**
         * The number of entries in the stack.
         */
        private int stackSize;

        /**
         * The queue of resident cold entries.
         * <p>
         * There is always at least one entry: the head entry.
         */
        private final Entry<V> queue;

        /**
         * The queue of non-resident cold entries.
         * <p>
         * There is always at least one entry: the head entry.
         */
        private final Entry<V> queue2;

        /**
         * 发生过多少趟有entry移动到stack顶部,对应entry的topMove <br>
         * The number of times any item was moved to the top of the stack.
         */
        private int stackMoveRoundCount;

        /**
         * Create a new cache segment.
         *
         * @param maxMemory                the maximum memory to use
         * @param stackMoveDistance        the number of other entries to be moved to
         *                                 the top of the stack before moving an entry to the top
         * @param len                      the number of hash table buckets (must be a power of 2)
         * @param nonResidentQueueSize     the non-resident queue size low watermark factor
         * @param nonResidentQueueSizeHigh the non-resident queue size high watermark factor
         */
        Segment(long maxMemory,
                int stackMoveDistance,
                int len,
                int nonResidentQueueSize,
                int nonResidentQueueSizeHigh) {
            this.maxMemory = maxMemory;
            this.stackMoveDistance = stackMoveDistance;
            this.nonResidentQueueSize = nonResidentQueueSize;
            this.nonResidentQueueSizeHigh = nonResidentQueueSizeHigh;

            // the bit mask has all bits set
            mask = len - 1;

            // initialize the stack and queue heads
            stack = new Entry<>();
            stack.stackPrev = stack.stackNext = stack;

            queue = new Entry<>();
            queue.queuePrev = queue.queueNext = queue;

            queue2 = new Entry<>();
            queue2.queuePrev = queue2.queueNext = queue2;

            @SuppressWarnings("unchecked")
            Entry<V>[] e = new Entry[len];
            entries = e;
        }

        /**
         * Create a new cache segment from an existing one.
         * The caller must synchronize on the old segment, to avoid
         * concurrent modifications.
         *
         * @param old the old segment
         * @param len the number of hash table buckets (must be a power of 2)
         */
        Segment(Segment<V> old, int len) {
            this(old.maxMemory, old.stackMoveDistance, len, old.nonResidentQueueSize, old.nonResidentQueueSizeHigh);

            hitCount = old.hitCount;
            missCount = old.missCount;

            Entry<V> entry = old.stack.stackPrev;
            while (entry != old.stack) {
                Entry<V> e = new Entry<>(entry);
                addToMap(e);
                addToStack(e);
                entry = entry.stackPrev;
            }

            entry = old.queue.queuePrev;
            while (entry != old.queue) {
                Entry<V> e = find(entry.key, getHash(entry.key));
                if (e == null) {
                    e = new Entry<>(entry);
                    addToMap(e);
                }
                addToQueue(queue, e);
                entry = entry.queuePrev;
            }

            entry = old.queue2.queuePrev;
            while (entry != old.queue2) {
                Entry<V> e = find(entry.key, getHash(entry.key));
                if (e == null) {
                    e = new Entry<>(entry);
                    addToMap(e);
                }
                addToQueue(queue2, e);
                entry = entry.queuePrev;
            }
        }

        /**
         * Calculate the new number of hash table buckets if the internal map should be re-sized.
         *
         * @return 0 if no resizing is needed, or the new length
         */
        int getNewMapLen() {
            int len = mask + 1;
            if (len * 3 < mapSize * 4 && len < (1 << 28)) {
                // more than 75% usage
                return len * 2;
            }

            if (len > 32 && len / 8 > mapSize) {
                // less than 12% usage
                return len / 2;
            }

            return 0;
        }

        private void addToMap(Entry<V> entry) {
            int index = getHash(entry.key) & mask;
            entry.mapNext = entries[index];
            entries[index] = entry;
            usedMemory += entry.getMemory();
            mapSize++;
        }

        /**
         * Get the value from the given entry.
         * This method adjusts the internal state of the cache sometimes,
         * to ensure commonly used entries stay in the cache.
         *
         * @param entry the entry
         * @return the value, or null if there is no resident entry
         */
        synchronized V get(Entry<V> entry) {
            V value = entry == null ? null : entry.getValue();

            // the entry was not found, or it was a non-resident entry
            if (value == null) {
                missCount++;
            } else {
                access(entry);
                hitCount++;
            }

            return value;
        }

        /**
         * Access an item, moving the entry to the top of the stack or front of the queue if found.
         */
        private void access(Entry<V> entry) {
            if (entry.isHot()) { // stack体系动手
                if (entry != stack.stackNext && entry.stackNext != null) {
                    // 需要移动到stack顶部
                    if (stackMoveRoundCount - entry.topMove > stackMoveDistance) {
                        // move a hot entry to the top of the stack unless it is already there
                        boolean wasEnd = entry == stack.stackPrev;

                        removeFromStack(entry);

                        // if moving the last entry, the last entry could now be cold, which is not allowed
                        if (wasEnd) {
                            pruneStack();
                        }

                        addToStack(entry);
                    }
                }
            } else { // queue体系动手
                V value = entry.getValue();
                if (value == null) {
                    return;
                }

                removeFromQueue(entry);

                // todo rust略过
                if (entry.weakReference != null) {
                    entry.value = value;
                    entry.weakReference = null;
                    usedMemory += entry.memory;
                }

                if (entry.stackNext != null) {
                    // resident, or even non-resident (weak value reference),
                    // cold entries become hot if they are on the stack
                    removeFromStack(entry);

                    // which means a hot entry needs to become cold
                    // (this entry is cold, that means there is at least one
                    // more entry in the stack, which must be hot)
                    convertOldestHotToCold();
                } else {
                    // cold entries that are not on the stack move to the front of the queue
                    addToQueue(queue, entry);
                }

                // in any case, the cold entry is moved to the top of the stack
                addToStack(entry);

                // but if newly promoted cold/non-resident is the only entry on a stack now
                // that means last one is cold, need to prune
                pruneStack();
            }
        }

        /**
         * Add an entry to the cache. The entry may or may not exist in the
         * cache yet. This method will usually mark unknown entries as cold and
         * known entries as hot.
         *
         * @param key    the key (may not be null)
         * @param hash   the hash
         * @param value  the value (may not be null)
         * @param memory the memory used for the given entry
         * @return the old value, or null if there was no resident entry
         */
        synchronized V put(long key, int hash, V value, int memory) {
            Entry<V> entry = find(key, hash);
            boolean existed = entry != null;

            V old = null;

            if (existed) {
                old = entry.getValue();
                remove(key, hash);
            }

            // the new entry is too big to fit
            if (memory > maxMemory) {
                return old;
            }

            entry = new Entry<>(key, value, memory);

            int index = hash & mask;

            entry.mapNext = entries[index];
            entries[index] = entry;

            usedMemory += memory;
            if (usedMemory > maxMemory) {
                // old entries needs to be removed
                evict();

                // if the cache is full, the new entry is cold if possible
                if (stackSize > 0) {
                    // the new cold entry is at the top of the queue
                    addToQueue(queue, entry);
                }
            }

            mapSize++;

            // added entries are always added to the stack
            addToStack(entry);
            if (existed) {
                // if it was there before (even non-resident), it becomes hot
                access(entry);
            }

            return old;
        }

        /**
         * Remove an entry. Both resident and non-resident entries can be removed.
         *
         * @param key  the key (may not be null)
         * @param hash the hash
         * @return the old value, or null if there was no resident entry
         */
        synchronized V remove(long key, int hash) {
            int index = hash & mask;

            Entry<V> entry = entries[index];
            if (entry == null) {
                return null;
            }

            if (entry.key == key) {
                entries[index] = entry.mapNext;
            } else {
                Entry<V> last;
                do {
                    last = entry;
                    entry = entry.mapNext;
                    if (entry == null) {
                        return null;
                    }
                } while (entry.key != key);

                last.mapNext = entry.mapNext;
            }

            V old = entry.getValue();

            mapSize--;
            usedMemory -= entry.getMemory();

            if (entry.stackNext != null) {
                removeFromStack(entry);
            }

            if (entry.isHot()) {
                // when removing a hot entry, the newest cold entry gets hot,
                // so the number of hot entries does not change
                entry = queue.queueNext;

                if (entry != queue) {
                    removeFromQueue(entry);

                    if (entry.stackNext == null) {
                        addToStackBottom(entry);
                    }
                }

                pruneStack();
            } else {
                removeFromQueue(entry);
            }

            return old;
        }

        /**
         * evict cold entries (resident and non-resident) until the memory limit
         * is reached. The new entry is added as a cold entry, except if it is the only entry.
         */
        private void evict() {
            do {
                evictBlock();
            } while (usedMemory > maxMemory);
        }

        private void evictBlock() {
            // ensure there are not too many hot entries: right shift of 5 is
            // division by 32, that means if there are only 1/32 (3.125%) or
            // less cold entries, a hot entry needs to become cold
            while (queueSize <= ((mapSize - queue2Size) >>> 5) && stackSize > 0) {
                convertOldestHotToCold();
            }

            // the oldest resident cold entries become non-resident
            while (usedMemory > maxMemory && queueSize > 0) {
                Entry<V> entry = queue.queuePrev;
                usedMemory -= entry.memory;

                removeFromQueue(entry);

                entry.weakReference = new WeakReference<>(entry.value);
                entry.value = null;

                addToQueue(queue2, entry);

                // the size of the non-resident-cold entries needs to be limited
                trimNonResidentQueue();
            }
        }

        private void convertOldestHotToCold() {
            // the last entry of the stack is known to be hot
            Entry<V> last = stack.stackPrev;

            // never remove the stack head itself,mean the internal structure of the cache is corrupt
            if (last == stack) {
                throw new IllegalStateException();
            }

            // remove from stack - which is done anyway in the stack pruning,but we can do it here as well
            removeFromStack(last);

            // adding an entry to the queue will make it cold
            addToQueue(queue, last);

            pruneStack();
        }

        void trimNonResidentQueue() {
            int residentCount = mapSize - queue2Size;

            int maxQueue2SizeHigh = nonResidentQueueSizeHigh * residentCount;
            int maxQueue2Size = nonResidentQueueSize * residentCount;

            while (queue2Size > maxQueue2Size) {
                Entry<V> entry = queue2.queuePrev;
                if (queue2Size <= maxQueue2SizeHigh) {
                    WeakReference<V> weakReference = entry.weakReference;
                    if (weakReference != null && weakReference.get() != null) {
                        break;  // stop trimming if entry holds a value
                    }
                }

                int hash = getHash(entry.key);
                remove(entry.key, hash);
            }
        }

        /**
         * Ensure the last entry of the stack is cold.
         */
        private void pruneStack() {
            while (true) {
                Entry<V> last = stack.stackPrev;

                // must stop at a hot entry or the stack head,
                // but the stack head itself is also hot, so we don't have to test it
                if (last.isHot()) {
                    break;
                }

                // the cold entry is still in the queue
                removeFromStack(last);
            }
        }

        /**
         * Try to find an entry in the map.
         *
         * @param key  the key
         * @param hash the hash
         * @return the entry (might be a non-resident)
         */
        Entry<V> find(long key, int hash) {
            int index = hash & mask;
            Entry<V> entry = entries[index];
            while (entry != null && entry.key != key) {
                entry = entry.mapNext;
            }
            return entry;
        }

        private void addToStack(Entry<V> entry) {
            entry.stackPrev = stack;
            entry.stackNext = stack.stackNext;
            entry.stackNext.stackPrev = entry;

            stack.stackNext = entry;
            stackSize++;

            entry.topMove = stackMoveRoundCount++;
        }

        private void addToStackBottom(Entry<V> entry) {
            entry.stackNext = stack;
            entry.stackPrev = stack.stackPrev;
            entry.stackPrev.stackNext = entry;
            stack.stackPrev = entry;
            stackSize++;
        }

        /**
         * Remove the entry from the stack. The head itself must not be removed.
         */
        private void removeFromStack(Entry<V> entry) {
            entry.stackPrev.stackNext = entry.stackNext;
            entry.stackNext.stackPrev = entry.stackPrev;
            entry.stackPrev = entry.stackNext = null;
            stackSize--;
        }

        private void addToQueue(Entry<V> queue, Entry<V> entry) {
            entry.queuePrev = queue;
            entry.queueNext = queue.queueNext;
            entry.queueNext.queuePrev = entry;
            queue.queueNext = entry;

            if (entry.value != null) {
                queueSize++;
            } else {
                queue2Size++;
            }
        }

        private void removeFromQueue(Entry<V> entry) {
            entry.queuePrev.queueNext = entry.queueNext;
            entry.queueNext.queuePrev = entry.queuePrev;
            entry.queuePrev = entry.queueNext = null;
            if (entry.value != null) {
                queueSize--;
            } else {
                queue2Size--;
            }
        }

        /**
         * Get the list of keys. This method allows to read the internal state of the cache.
         *
         * @param cold        if true, only keys for the cold entries are returned
         * @param nonResident true for non-resident entries
         * @return the key list
         */
        synchronized List<Long> keys(boolean cold, boolean nonResident) {
            ArrayList<Long> keys = new ArrayList<>();
            if (cold) {
                Entry<V> start = nonResident ? queue2 : queue;
                for (Entry<V> e = start.queueNext; e != start;
                     e = e.queueNext) {
                    keys.add(e.key);
                }
            } else {
                for (Entry<V> e = stack.stackNext; e != stack;
                     e = e.stackNext) {
                    keys.add(e.key);
                }
            }
            return keys;
        }

        /**
         * Get the set of keys for resident entries.
         *
         * @return the set of keys
         */
        synchronized Set<Long> keySet() {
            HashSet<Long> set = new HashSet<>();
            for (Entry<V> e = stack.stackNext; e != stack; e = e.stackNext) {
                set.add(e.key);
            }
            for (Entry<V> e = queue.queueNext; e != queue; e = e.queueNext) {
                set.add(e.key);
            }
            return set;
        }
    }

    /**
     * A cache entry. Each entry is either hot (low inter-reference recency;
     * LIR), cold (high inter-reference recency; HIR), or non-resident-cold. Hot
     * entries are in the stack only. Cold entries are in the queue, and may be
     * in the stack. Non-resident-cold entries have their value set to null and
     * are in the stack and in the non-resident queue.
     *
     * @param <V> the value type
     */
    static class Entry<V> {

        /**
         * The key.
         */
        final long key;

        /**
         * The value. Set to null for non-resident-cold entries.
         */
        V value;

        /**
         * Weak reference to the value. Set to null for resident entries.
         */
        WeakReference<V> weakReference;

        /**
         * The estimated memory used.
         */
        final int memory;

        /**
         * 是在对应隶属的segment第几趟有entry移动到stack顶部时候该entry移动到顶部的<br>
         * When the item was last moved to the top of the stack.
         */
        int topMove;

        /**
         * The next entry in the stack.
         */
        Entry<V> stackNext;

        /**
         * The previous entry in the stack.
         */
        Entry<V> stackPrev;

        /**
         * The next entry in the queue (either the resident queue or the non-resident queue).
         */
        Entry<V> queueNext;

        /**
         * The previous entry in the queue.
         */
        Entry<V> queuePrev;

        /**
         * The next entry in the map (the chained entry).
         */
        Entry<V> mapNext;

        Entry() {
            this(0L, null, 0);
        }

        Entry(long key, V value, int memory) {
            this.key = key;
            this.memory = memory;
            this.value = value;
        }

        Entry(Entry<V> old) {
            this(old.key, old.value, old.memory);
            this.weakReference = old.weakReference;
            this.topMove = old.topMove;
        }

        /**
         * Whether this entry is hot. Cold entries are in one of the two queues.
         */
        boolean isHot() {
            return queueNext == null;
        }

        V getValue() {
            return value == null ? weakReference.get() : value;
        }

        int getMemory() {
            return value == null ? 0 : memory;
        }
    }

    public static class Config {

        /**
         * The maximum memory to use (1 or larger).
         */
        public long maxMemory = 1;

        /**
         * The number of cache segments (must be a power of 2).
         */
        public int segmentCount = 16;

        /**
         * How many other item are to be moved to the top of the stack before
         * the current item is moved.
         */
        public int stackMoveDistance = 32;

        /**
         * Low water mark for the number of entries in the non-resident queue,
         * as a factor of the number of all other entries in the map.
         */
        public final int nonResidentQueueSize = 3;

        /**
         * High watermark for the number of entries in the non-resident queue,
         * as a factor of the number of all other entries in the map
         */
        public final int nonResidentQueueSizeHigh = 12;
    }
}
