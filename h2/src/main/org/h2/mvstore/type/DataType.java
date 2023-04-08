/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.type;


import java.nio.ByteBuffer;
import java.util.Comparator;

import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;

public interface DataType<T> extends Comparator<T> {

    /**
     * Compare two keys.
     *
     * @param a the first key
     * @param b the second key
     * @return -1 if the first key is smaller, 1 if larger, and 0 if equal
     * @throws UnsupportedOperationException if the type is not orderable
     */
    @Override
    default int compare(T a, T b) {
        throw DataUtils.newUnsupportedOperationException("Can not compare");
    }

    /**
     * Perform binary search for the key within the storage
     *
     * @param key          to search for
     * @param storage      to search within (an array of type T)
     * @param size         number of data items in the storage
     * @param initialGuess for key position
     * @return index of the key , if found, - index of the insertion point, if not
     */
    default int binarySearch(T key, T[] storage, int size, int initialGuess) {
        int low = 0;
        int high = size - 1;

        // the cached index minus one, so that
        // for the first time (when cachedCompare is 0),
        // the default value is used
        int x = initialGuess - 1;
        if (x < 0 || x > high) {
            x = high >>> 1;
        }

        while (low <= high) {
            int compare = compare(key, storage[x]);
            if (compare > 0) {
                low = x + 1;
            } else if (compare < 0) {
                high = x - 1;
            } else {
                return x;
            }
            x = (low + high) >>> 1;
        }

        return ~low;
    }

    /**
     * Calculates the amount of used memory in bytes.
     */
    int getMemory(T obj);

    /**
     * Whether memory estimation based on previously seen values is allowed/desirable
     */
    default boolean isMemoryEstimationAllowed() {
        return true;
    }

    /**
     * Write an object.
     *
     * @param writeBuffer the target buffer
     * @param obj         the value
     */
    void write(WriteBuffer writeBuffer, T obj);

    /**
     * Write a list of objects.
     *
     * @param writeBuffer    the target buffer
     * @param storage the objects
     * @param len     the number of objects to write
     */
    default void write(WriteBuffer writeBuffer, T[] storage, int len) {
        for (int i = 0; i < len; i++) {
            write(writeBuffer, storage[i]);
        }
    }

    /**
     * Read an object.
     *
     * @param byteBuffer the source buffer
     * @return the object
     */
    T read(ByteBuffer byteBuffer);

    /**
     * Read a list of objects.
     *
     * @param byteBuffer    the target buffer
     * @param storage the objects
     * @param len     the number of objects to read
     */
    default void read(ByteBuffer byteBuffer, T[] storage, int len) {
        for (int i = 0; i < len; i++) {
            storage[i] = read(byteBuffer);
        }
    }

    /**
     * Create storage object of array type to hold values
     *
     * @param size number of values to hold
     * @return storage object
     */
    T[] createStorage(int size);

    default T[] cast(Object storage) {
        return (T[]) storage;
    }
}

