/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import static org.h2.engine.Constants.MEMORY_ARRAY;
import static org.h2.engine.Constants.MEMORY_OBJECT;
import static org.h2.engine.Constants.MEMORY_POINTER;
import static org.h2.mvstore.DataUtils.PAGE_TYPE_LEAF;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.h2.compress.Compressor;
import org.h2.util.Utils;

/**
 * A page (a node or a leaf).
 * <p>
 * For b-tree nodes, the key at a given index is larger than the largest key of
 * the child at the same index.
 * <p>
 * Serialized format:
 * length of a serialized page in bytes (including this field): int
 * check value: short
 * page number (0-based sequential number within a chunk): varInt
 * map id: varInt
 * number of keys: varInt
 * type: byte (0: leaf, 1: node; +2: compressed)
 * children of the non-leaf node (1 more than keys)
 * compressed: bytes saved (varInt)
 * keys
 * values of the leaf node (one for each key)
 */
public abstract class Page<K, V> implements Cloneable {

    /**
     * Map this page belongs to 外边传进来的的
     */
    public final MVMap<K, V> mvMap;

    /**
     * Position of this page's saved image within a Chunk
     * or 0 if this page has not been saved yet
     * or 1 if this page has not been saved yet, but already removed
     * This "removed" flag is to keep track of pages that concurrently
     * changed while they are being stored, in which case the live bookkeeping
     * needs to be aware of such cases.
     * Field need to be volatile to avoid races between saving thread setting it
     * and other thread reading it to access the page.
     * On top of this update atomicity is required so removal mark and saved position
     * can be set concurrently.
     *
     * @see DataUtils#getPagePos(int, int, int, int) for field format details
     */
    public volatile long position;

    /**
     * Sequential 0-based number of the page within containing chunk.
     */
    public int pageNo = -1;

    /**
     * The last result of a find operation is cached.
     */
    private int cachedCompare;

    /**
     * The estimated memory used in persistent case, IN_MEMORY marker value otherwise.
     */
    private int memory;

    /**
     * amount of used disk space by this page only in persistent case.
     */
    private int diskSpaceUsed;

    /**
     * The keys.
     */
    private K[] keys;

    /**
     * Updater for pos field, which can be updated when page is saved,
     * but can be concurrently marked as removed
     */
    @SuppressWarnings("rawtypes")
    private static final AtomicLongFieldUpdater<Page> posUpdater = AtomicLongFieldUpdater.newUpdater(Page.class, "position");

    /**
     * The estimated number of bytes used per child entry.
     */
    static final int PAGE_MEMORY_CHILD = MEMORY_POINTER + 16; //  16 = two longs

    /**
     * The estimated number of bytes used per base page.
     */
    private static final int PAGE_MEMORY =
            MEMORY_OBJECT +           // this
                    2 * MEMORY_POINTER +      // map, keys
                    MEMORY_ARRAY +            // Object[] keys
                    17;                       // pos, cachedCompare, memory, removedInMemory
    /**
     * The estimated number of bytes used per empty internal page object.
     */
    static final int PAGE_NODE_MEMORY =
            PAGE_MEMORY +             // super
                    MEMORY_POINTER +          // children
                    MEMORY_ARRAY +            // Object[] children
                    8;                        // totalCount

    /**
     * The estimated number of bytes used per empty leaf page.
     */
    static final int PAGE_LEAF_MEMORY =
            PAGE_MEMORY +             // super
                    MEMORY_POINTER +          // values
                    MEMORY_ARRAY;             // Object[] values

    /**
     * Marker value for memory field, meaning that memory accounting is replaced by key count.
     */
    private static final int IN_MEMORY = Integer.MIN_VALUE;

    @SuppressWarnings("rawtypes")
    private static final PageReference[] SINGLE_EMPTY = {PageReference.EMPTY};

    Page(MVMap<K, V> mvMap) {
        this.mvMap = mvMap;
    }

    Page(MVMap<K, V> mvMap, Page<K, V> source) {
        this(mvMap, source.keys);
        memory = source.memory;
    }

    Page(MVMap<K, V> mvMap, K[] keys) {
        this.mvMap = mvMap;
        this.keys = keys;
    }

    /**
     * Create a new, empty leaf page.
     *
     * @param <K>   key type
     * @param <V>   value type
     * @param mvMap the map
     * @return the new page
     */
    static <K, V> Page<K, V> createEmptyLeaf(MVMap<K, V> mvMap) {
        return createLeaf(mvMap, mvMap.keyType.createStorage(0), mvMap.valueType.createStorage(0), PAGE_LEAF_MEMORY);
    }

    /**
     * Create a new, empty internal node page.
     *
     * @param <K> key type
     * @param <V> value type
     * @param map the map
     * @return the new page
     */
    @SuppressWarnings("unchecked")
    static <K, V> Page<K, V> createEmptyNoLeaf(MVMap<K, V> map) {
        return createNoLeaf(
                map,
                map.getKeyType().createStorage(0),
                SINGLE_EMPTY,
                0,
                PAGE_NODE_MEMORY + MEMORY_POINTER + PAGE_MEMORY_CHILD); // there is always one child
    }

    /**
     * Create a new non-leaf page. The arrays are not cloned.
     *
     * @param <K>        the key class
     * @param <V>        the value class
     * @param map        the map
     * @param keys       the keys
     * @param children   the child page positions
     * @param totalCount the total number of keys
     * @param memory     the memory used in bytes
     * @return the page
     */
    public static <K, V> Page<K, V> createNoLeaf(MVMap<K, V> map,
                                                 K[] keys,
                                                 PageReference<K, V>[] children,
                                                 long totalCount,
                                                 int memory) {
        assert keys != null;
        Page<K, V> page = new NonLeaf<>(map, keys, children, totalCount);
        page.initMemoryAccount(memory);
        return page;
    }

    /**
     * Create a new leaf page. The arrays are not cloned.
     *
     * @param <K>    key type
     * @param <V>    value type
     * @param map    the map
     * @param keys   the keys
     * @param values the values
     * @param memory the memory used in bytes
     * @return the page
     */
    static <K, V> Page<K, V> createLeaf(MVMap<K, V> map, K[] keys, V[] values, int memory) {
        assert keys != null;
        Page<K, V> page = new Leaf<>(map, keys, values);
        page.initMemoryAccount(memory);
        return page;
    }

    private void initMemoryAccount(int memoryCount) {
        if (!mvMap.isPersistent()) {
            memory = IN_MEMORY;
        } else if (memoryCount == 0) {
            recalculateMemory();
        } else {
            addMemory(memoryCount);
            assert memoryCount == getMemory();
        }
    }

    /**
     * Get the value for the given key, or null if not found.
     * Search is done in the tree rooted at given page.
     *
     * @param <K>  key type
     * @param <V>  value type
     * @param key  the key
     * @param page the root page
     * @return the value, or null if not found
     */
    static <K, V> V get(Page<K, V> page, K key) {
        while (true) {
            int index = page.binarySearch(key);

            if (page.isLeaf()) {
                return index >= 0 ? page.getValue(index) : null;
            }

            if (index++ < 0) {
                index = -index;
            }

            page = page.getChildPage(index);
        }
    }

    /**
     * Read a page.
     *
     * @param <K>        key type
     * @param <V>        value type
     * @param byteBuffer ByteBuffer containing serialized page info
     * @param position   the position
     * @param mvMap      the map
     * @return the page
     */
    static <K, V> Page<K, V> readFromByteBuffer(ByteBuffer byteBuffer, long position, MVMap<K, V> mvMap) {
        boolean leaf = (DataUtils.getPageType(position) & 1) == PAGE_TYPE_LEAF;
        Page<K, V> page = leaf ? new Leaf<>(mvMap) : new NonLeaf<>(mvMap);
        page.position = position;
        page.readFromByteBuffer(byteBuffer);
        return page;
    }

    /**
     * Get the id of the page's owner map
     */
    public final int getMapId() {
        return mvMap.getId();
    }

    /**
     * Create a copy of this page with potentially different owning map.
     * This is used exclusively during bulk map copying.
     * Child page references for nodes are cleared (re-pointed to an empty page)
     * to be filled-in later to copying procedure. This way it can be saved
     * mid-process without tree integrity violation
     *
     * @param mvMap             new map to own resulting page
     * @param eraseChildrenRefs whether cloned Page should have no child references or keep originals
     * @return the page
     */
    abstract Page<K, V> copy(MVMap<K, V> mvMap, boolean eraseChildrenRefs);

    /**
     * Get the key at the given index.
     *
     * @param index the index
     * @return the key
     */
    public K getKey(int index) {
        return keys[index];
    }

    public abstract Page<K, V> getChildPage(int index);

    /**
     * Get the position of the child.
     *
     * @return the position
     */
    public abstract long getChildPagePos(int index);

    /**
     * get the value at the given index.
     */
    public abstract V getValue(int index);

    public final int getKeyCount() {
        return keys.length;
    }

    public final boolean isLeaf() {
        return getNodeType() == PAGE_TYPE_LEAF;
    }

    public abstract int getNodeType();

    public final long getPosition() {
        return position;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        dump(buff);
        return buff.toString();
    }

    /**
     * Dump debug data for this page.
     *
     * @param buff append buffer
     */
    protected void dump(StringBuilder buff) {
        buff.append("id: ").append(System.identityHashCode(this)).append('\n');
        buff.append("pos: ").append(Long.toHexString(position)).append('\n');
        if (isSaved()) {
            int chunkId = DataUtils.getPageChunkId(position);
            buff.append("chunk: ").append(Long.toHexString(chunkId)).append('\n');
        }
    }

    /**
     * Create a copy of this page.
     *
     * @return a mutable copy of this page
     */
    public final Page<K, V> copy() {
        Page<K, V> newPage = clone();
        newPage.position = 0;
        return newPage;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected final Page<K, V> clone() {
        Page<K, V> clone;
        try {
            clone = (Page<K, V>) super.clone();
        } catch (CloneNotSupportedException impossible) {
            throw new RuntimeException(impossible);
        }
        return clone;
    }

    /**
     * Search the key in this page using a binary search. Instead of always
     * starting the search in the middle, the last found index is cached.
     * <p>
     * If the key was found, the returned value is the index in the key array.
     * If not found, the returned value is negative, where -1 means the provided
     * key is smaller than any keys in this page. See also Arrays.binarySearch.
     *
     * @param key the key
     * @return the value or null
     */
    int binarySearch(K key) {
        int res = mvMap.getKeyType().binarySearch(key, keys, getKeyCount(), cachedCompare);
        cachedCompare = res < 0 ? ~res : res + 1;
        return res;
    }

    /**
     * Split the page. This modifies the current page.
     *
     * @param at the split index
     * @return the page with the entries after the split index
     */
    abstract Page<K, V> split(int at);

    /**
     * Split the current keys array into two arrays.
     *
     * @param aCount size of the first array.
     * @param bCount size of the second array/
     * @return the second array.
     */
    final K[] splitKeys(int aCount, int bCount) {
        assert aCount + bCount <= getKeyCount();
        K[] aKeys = createKeyStorage(aCount);
        K[] bKeys = createKeyStorage(bCount);
        System.arraycopy(keys, 0, aKeys, 0, aCount);
        System.arraycopy(keys, getKeyCount() - bCount, bKeys, 0, bCount);
        keys = aKeys;
        return bKeys;
    }

    /**
     * Append additional key/value mappings to this Page.
     * New mappings suppose to be in correct key order.
     *
     * @param extraKeyCount number of mappings to be added
     * @param extraKeys     to be added
     * @param extraValues   to be added
     */
    abstract void expand(int extraKeyCount, K[] extraKeys, V[] extraValues);

    /**
     * Expand the keys array.
     *
     * @param extraKeyCount number of extra key entries to create
     * @param extraKeys     extra key values
     */
    final void expandKeys(int extraKeyCount, K[] extraKeys) {
        int keyCount = getKeyCount();
        K[] newKeys = createKeyStorage(keyCount + extraKeyCount);
        System.arraycopy(keys, 0, newKeys, 0, keyCount);
        System.arraycopy(extraKeys, 0, newKeys, keyCount, extraKeyCount);
        keys = newKeys;
    }

    /**
     * Get the total number of key-value pairs, including child pages.
     */
    public abstract long getTotalCount();

    /**
     * Get the number of key-value pairs for a given child.
     *
     * @param index the child index
     * @return the descendant count
     */
    abstract long getCounts(int index);

    /**
     * Replace the child page.
     *
     * @param index     the index
     * @param childPage the new child page
     */
    public abstract void setChild(int index, Page<K, V> childPage);

    /**
     * Replace the key at an index in this page.
     *
     * @param index the index
     * @param key   the new key
     */
    public final void setKey(int index, K key) {
        keys = keys.clone();
        if (isPersistent()) {
            K old = keys[index];
            if (!mvMap.isMemoryEstimationAllowed() || old == null) {
                int mem = mvMap.evaluateMemoryForKey(key);
                if (old != null) {
                    mem -= mvMap.evaluateMemoryForKey(old);
                }
                addMemory(mem);
            }
        }
        keys[index] = key;
    }

    /**
     * Replace the value at an index in this page.
     *
     * @param index the index
     * @param value the new value
     * @return the old value
     */
    public abstract V setValue(int index, V value);

    /**
     * insert a key-value pair into leaf.
     *
     * @param index the index
     * @param key   the key
     * @param value the value
     */
    public abstract void insertLeaf(int index, K key, V value);

    /**
     * insert a child page into non-leaf.
     *
     * @param index     the index
     * @param key       the key
     * @param childPage the child page
     */
    public abstract void insertNode(int index, K key, Page<K, V> childPage);

    /**
     * Insert a key into the key array
     *
     * @param index index to insert at
     * @param key   the key value
     */
    final void insertKey(int index, K key) {
        int keyCount = getKeyCount();
        assert index <= keyCount : index + " > " + keyCount;
        K[] newKeys = createKeyStorage(keyCount + 1);
        DataUtils.copyWithGap(keys, newKeys, keyCount, index);
        keys = newKeys;

        keys[index] = key;

        if (isPersistent()) {
            addMemory(MEMORY_POINTER + mvMap.evaluateMemoryForKey(key));
        }
    }

    /**
     * Remove the key and value (or child) at the given index.
     */
    public void remove(int index) {
        int keyCount = getKeyCount();
        if (index == keyCount) {
            --index;
        }
        if (isPersistent()) {
            if (!mvMap.isMemoryEstimationAllowed()) {
                K old = getKey(index);
                addMemory(-MEMORY_POINTER - mvMap.evaluateMemoryForKey(old));
            }
        }
        K[] newKeys = createKeyStorage(keyCount - 1);
        DataUtils.copyExcept(keys, newKeys, keyCount, index);
        keys = newKeys;
    }

    /**
     * Read the page from the buffer.
     *
     * @param byteBuffer the buffer to read from
     */
    private void readFromByteBuffer(ByteBuffer byteBuffer) {
        int chunkId = DataUtils.getPageChunkId(position);
        int offset = DataUtils.getPageOffsetInChunk(position);

        int start = byteBuffer.position();
        int pageLength = byteBuffer.getInt(); // does not include optional part (pageNo)
        int remaining = byteBuffer.remaining() + 4;
        if (pageLength > remaining || pageLength < 4) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_FILE_CORRUPT, "File corrupted in chunk {0}, expected page length 4..{1}, got {2}", chunkId, remaining, pageLength);
        }

        short check = byteBuffer.getShort();
        int checkTest = DataUtils.getCheckValue(chunkId) ^ DataUtils.getCheckValue(offset) ^ DataUtils.getCheckValue(pageLength);
        if (check != (short) checkTest) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_FILE_CORRUPT, "File corrupted in chunk {0}, expected check value {1}, got {2}", chunkId, checkTest, check);
        }

        // pageNo
        pageNo = DataUtils.readVarInt(byteBuffer);
        if (pageNo < 0) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_FILE_CORRUPT, "File corrupted in chunk {0}, got negative page No {1}", chunkId, pageNo);
        }

        // mapId
        int mvMapId = DataUtils.readVarInt(byteBuffer);
        if (mvMapId != mvMap.getId()) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_FILE_CORRUPT, "File corrupted in chunk {0}, expected map id {1}, got {2}", chunkId, mvMap.getId(), mvMapId);
        }

        // keyCount
        int keyCount = DataUtils.readVarInt(byteBuffer);
        keys = createKeyStorage(keyCount);
        int type = byteBuffer.get();
        if (isLeaf() != ((type & 1) == PAGE_TYPE_LEAF)) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_FILE_CORRUPT, "File corrupted in chunk {0}, expected node type {1}, got {2}", chunkId, isLeaf() ? "0" : "1", type);
        }

        // to restrain hacky GenericDataType, which grabs the whole remainder of the buffer
        byteBuffer.limit(start + pageLength);

        if (!isLeaf()) {
            readPayLoad(byteBuffer);
        }

        // todo rust略过
        boolean compressed = (type & DataUtils.PAGE_COMPRESSED) != 0;
        if (compressed) {
            Compressor compressor;
            if ((type & DataUtils.PAGE_COMPRESSED_HIGH) == DataUtils.PAGE_COMPRESSED_HIGH) {
                compressor = mvMap.getMvStore().getCompressorHigh();
            } else {
                compressor = mvMap.getMvStore().getCompressorFast();
            }

            int lenAdd = DataUtils.readVarInt(byteBuffer);
            int compLen = byteBuffer.remaining();
            byte[] comp;
            int pos = 0;
            if (byteBuffer.hasArray()) {
                comp = byteBuffer.array();
                pos = byteBuffer.arrayOffset() + byteBuffer.position();
            } else {
                comp = Utils.newBytes(compLen);
                byteBuffer.get(comp);
            }
            int l = compLen + lenAdd;
            byteBuffer = ByteBuffer.allocate(l);
            compressor.expand(comp, pos, compLen, byteBuffer.array(), byteBuffer.arrayOffset(), l);
        }

        mvMap.getKeyType().read(byteBuffer, keys, keyCount);

        if (isLeaf()) {
            readPayLoad(byteBuffer);
        }

        diskSpaceUsed = pageLength;

        recalculateMemory();
    }

    /**
     * Read the page payload from the buffer.
     *
     * @param byteBuffer the buffer
     */
    protected abstract void readPayLoad(ByteBuffer byteBuffer);

    public final boolean isSaved() {
        return DataUtils.isPageSaved(position);
    }

    public final boolean isRemoved() {
        return DataUtils.isPageRemoved(position);
    }

    /**
     * Mark this page as removed "in memory". That means that only adjustment of
     * "unsaved memory" amount is required. On the other hand, if page was
     * persisted, it's removal should be reflected in occupancy of the
     * containing chunk.
     *
     * @return true if it was marked by this call or has been marked already,
     * false if page has been saved already.
     */
    private boolean markAsRemoved() {
        assert getTotalCount() > 0 : this;
        long pagePos;
        do {
            pagePos = position;
            if (DataUtils.isPageSaved(pagePos)) {
                return false;
            }
            assert !DataUtils.isPageRemoved(pagePos);
        } while (!posUpdater.compareAndSet(this, 0L, 1L));
        return true;
    }

    /**
     * Store the page and update the position.
     *
     * @param chunk       the chunk
     * @param writeBuffer the target buffer
     * @param toc         prospective table of content
     * @return the position of the buffer just after the type
     */
    protected final int write(Chunk chunk, WriteBuffer writeBuffer, List<Long> toc) {
        pageNo = toc.size();

        int keyCount = keys.length;
        int startPos = writeBuffer.position();

        writeBuffer.putInt(0)          // placeholder for pageLength
                .putShort((byte) 0) // placeholder for check
                .putVarInt(pageNo)
                .putVarInt(mvMap.getId())
                .putVarInt(keyCount);

        // page的type
        int typePos = writeBuffer.position();
        int type = isLeaf() ? PAGE_TYPE_LEAF : DataUtils.PAGE_TYPE_NODE;
        writeBuffer.put((byte) type);

        // children
        int childrenPos = writeBuffer.position();
        writeChildren(writeBuffer, true);

        // keys values
        int compressStartPos = writeBuffer.position();
        mvMap.getKeyType().write(writeBuffer, keys, keyCount);
        writeValues(writeBuffer);

        int expLen = writeBuffer.position() - compressStartPos;
        if (expLen > 16) { // 需要压缩
            int compressionLevel = mvMap.mvStore.getCompressionLevel();
            if (compressionLevel > 0) {
                Compressor compressor;
                int compressType;
                if (compressionLevel == 1) {
                    compressor = mvMap.mvStore.getCompressorFast();
                    compressType = DataUtils.PAGE_COMPRESSED;
                } else {
                    compressor = mvMap.mvStore.getCompressorHigh();
                    compressType = DataUtils.PAGE_COMPRESSED_HIGH;
                }
                byte[] comp = new byte[expLen * 2];
                ByteBuffer byteBuffer = writeBuffer.getBuffer();
                int pos = 0;
                byte[] exp;
                if (byteBuffer.hasArray()) {
                    exp = byteBuffer.array();
                    pos = byteBuffer.arrayOffset() + compressStartPos;
                } else {
                    exp = Utils.newBytes(expLen);
                    writeBuffer.position(compressStartPos).get(exp);
                }
                int compLen = compressor.compress(exp, pos, expLen, comp, 0);
                int plus = DataUtils.getVarIntLen(expLen - compLen);
                if (compLen + plus < expLen) {
                    writeBuffer.position(typePos).put((byte) (type | compressType));
                    writeBuffer.position(compressStartPos).putVarInt(expLen - compLen).put(comp, 0, compLen);
                }
            }
        }

        int pageLength = writeBuffer.position() - startPos;

        // 编码4个的元素的
        long tocElement = DataUtils.getTocElement(getMapId(), startPos, pageLength, type);
        toc.add(tocElement);

        // crc
        int check = DataUtils.getCheckValue(chunk.id) ^ DataUtils.getCheckValue(startPos) ^ DataUtils.getCheckValue(pageLength);
        writeBuffer.putInt(startPos, pageLength).putShort(startPos + 4, (short) check);

        if (isSaved()) {
            throw DataUtils.newMVStoreException(DataUtils.ERROR_INTERNAL, "page already stored");
        }

        long pagePos = DataUtils.getPagePos(chunk.id, tocElement);

        boolean isDeleted = isRemoved();
        while (!posUpdater.compareAndSet(this, isDeleted ? 1L : 0L, pagePos)) {
            isDeleted = isRemoved();
        }

        mvMap.mvStore.cachePage(this);

        // cache again - this will make sure nodes stays in the cache for a longer time
        if (type == DataUtils.PAGE_TYPE_NODE) {
            mvMap.mvStore.cachePage(this);
        }

        int pageLengthEncoded = DataUtils.getPageMaxLength(position);

        boolean singleWriter = mvMap.isSingleWriter();
        chunk.accountForWrittenPage(pageLengthEncoded, singleWriter);

        if (isDeleted) {
            mvMap.mvStore.accountForRemovedPage(pagePos, chunk.version + 1, singleWriter, pageNo);
        }

        diskSpaceUsed = pageLengthEncoded != DataUtils.PAGE_LARGE ? pageLengthEncoded : pageLength;

        return childrenPos;
    }

    /**
     * Write values that the buffer contains to the buff.
     *
     * @param buff the target buffer
     */
    protected abstract void writeValues(WriteBuffer buff);

    /**
     * Write page children to the buff.
     *
     * @param buff       the target buffer
     * @param withCounts true if the descendant counts should be written
     */
    protected abstract void writeChildren(WriteBuffer buff, boolean withCounts);

    /**
     * Store this page and all children that are changed, in reverse order, and
     * update the position and the children.
     *
     * @param chunk       the chunk
     * @param writeBuffer the target buffer
     * @param toc         prospective table of content
     */
    abstract void writeUnsavedRecursive(Chunk chunk, WriteBuffer writeBuffer, List<Long> toc);

    /**
     * Unlink the children recursively after all data is written.
     */
    abstract void releaseSavedPages();

    public abstract int getRawChildPageCount();

    protected final boolean isPersistent() {
        return memory != IN_MEMORY;
    }

    public final int getMemory() {
        if (isPersistent()) {
            return memory;
        }

        return 0;
    }

    /**
     * Amount of used disk space in persistent case including child pages.
     *
     * @return amount of used disk space in persistent case
     */
    public long getDiskSpaceUsed() {
        long r = 0;
        if (isPersistent()) {
            r += diskSpaceUsed;
            if (!isLeaf()) {
                for (int i = 0; i < getRawChildPageCount(); i++) {
                    long pos = getChildPagePos(i);
                    if (pos != 0) {
                        r += getChildPage(i).getDiskSpaceUsed();
                    }
                }
            }
        }
        return r;
    }

    /**
     * Increase estimated memory used in persistent case.
     */
    final void addMemory(int mem) {
        memory += mem;
        assert memory >= 0;
    }

    /**
     * Recalculate estimated memory used in persistent case.
     */
    final void recalculateMemory() {
        assert isPersistent();
        memory = calculateMemory();
    }

    /**
     * Calculate estimated memory used in persistent case.
     *
     * @return memory in byte
     */
    protected int calculateMemory() {

        return mvMap.evaluateMemoryForKeys(keys, getKeyCount());
/*
        int keyCount = getKeyCount();
        int mem = keyCount * MEMORY_POINTER;
        DataType<K> keyType = map.getKeyType();
        for (int i = 0; i < keyCount; i++) {
            mem += getMemory(keyType, keys[i]);
        }
        return mem;
*/
    }

    public boolean isComplete() {
        return true;
    }

    /**
     * Called when done with copying page.
     */
    public void setComplete() {
    }

    /**
     * Make accounting changes (chunk occupancy or "unsaved" RAM), related to
     * this page removal.
     *
     * @param version at which page was removed
     * @return amount (negative), by which "unsaved memory" should be adjusted,
     * if page is unsaved one, and 0 for page that was already saved, or
     * in case of non-persistent map
     */
    public final int removePage(long version) {
        if (isPersistent() && getTotalCount() > 0) {
            MVStore store = mvMap.mvStore;
            if (!markAsRemoved()) { // only if it has been saved already
                long pagePos = position;
                store.accountForRemovedPage(pagePos, version, mvMap.isSingleWriter(), pageNo);
            } else {
                return -memory;
            }
        }
        return 0;
    }

    /**
     * Extend path from a given CursorPos chain to "prepend point" in a B-tree, rooted at this Page.
     *
     * @param cursorPos presumably pointing to this Page (null if real root), to build upon
     * @return new head of the CursorPos chain
     */
    public abstract CursorPos<K, V> getPrependCursorPos(CursorPos<K, V> cursorPos);

    /**
     * Extend path from a given CursorPos chain to "append point" in a B-tree, rooted at this Page.
     *
     * @param cursorPos presumably pointing to this Page (null if real root), to build upon
     * @return new head of the CursorPos chain
     */
    public abstract CursorPos<K, V> getAppendCursorPos(CursorPos<K, V> cursorPos);

    /**
     * Remove all page data recursively.
     *
     * @param version at which page got removed
     * @return adjustment for "unsaved memory" amount
     */
    public abstract int removeAllRecursive(long version);

    /**
     * Create array for keys storage.
     *
     * @param size number of entries
     * @return values array
     */
    public final K[] createKeyStorage(int size) {
        return mvMap.getKeyType().createStorage(size);
    }

    /**
     * Create array for values storage.
     *
     * @param size number of entries
     * @return values array
     */
    final V[] createValueStorage(int size) {
        return mvMap.getValueType().createStorage(size);
    }

    @SuppressWarnings("unchecked")
    public static <K, V> PageReference<K, V>[] createRefStorage(int size) {
        return new PageReference[size];
    }

    /**
     * A pointer to a page, either in-memory or using a page position.
     */
    public static final class PageReference<K, V> {

        /**
         * Singleton object used when arrays of PageReference have not yet been filled.
         */
        @SuppressWarnings("rawtypes")
        static final PageReference EMPTY = new PageReference<>(null, 0, 0);

        /**
         * The position, if known, or 0.
         */
        private long position;

        /**
         * The page, if in memory, or null.
         */
        public Page<K, V> page;

        /**
         * The descendant count for this child page.
         */
        final long count;

        /**
         * Get an empty page reference.
         *
         * @param <X> the key class
         * @param <Y> the value class
         * @return the page reference
         */
        @SuppressWarnings("unchecked")
        public static <X, Y> PageReference<X, Y> empty() {
            return EMPTY;
        }

        public PageReference(Page<K, V> page) {
            this(page, page.getPosition(), page.getTotalCount());
        }

        PageReference(long position, long count) {
            this(null, position, count);
            assert DataUtils.isPageSaved(position);
        }

        private PageReference(Page<K, V> page, long position, long count) {
            this.page = page;
            this.position = position;
            this.count = count;
        }

        /**
         * Clear if necessary, reference to the actual child Page object,
         * so it can be garbage collected if not actively used elsewhere.
         * Reference is cleared only if corresponding page was already saved on a disk.
         */
        void clearPageReference() {
            if (page == null) {
                return;
            }

            page.releaseSavedPages();

            assert page.isSaved() || !page.isComplete();

            if (page.isSaved()) {
                assert position == page.getPosition();
                assert count == page.getTotalCount() : count + " != " + page.getTotalCount();
                page = null;
            }
        }

        long getPosition() {
            return position;
        }

        /**
         * Re-acquire position from in-memory page.
         */
        void resetPos() {
            Page<K, V> p = page;
            if (p != null && p.isSaved()) {
                position = p.getPosition();
                assert count == p.getTotalCount();
            }
        }

        @Override
        public String toString() {
            return "Cnt:" + count + ", pos:" + (position == 0 ? "0" : DataUtils.getPageChunkId(position) +
                    (page == null ? "" : "/" + page.pageNo) +
                    "-" + DataUtils.getPageOffsetInChunk(position) + ":" + DataUtils.getPageMaxLength(position)) +
                    ((page == null ? DataUtils.getPageType(position) == 0 : page.isLeaf()) ? " leaf" : " node") +
                    ", page:{" + page + "}";
        }
    }

    private static class NonLeaf<K, V> extends Page<K, V> {
        /**
         * The child page references.
         */
        private PageReference<K, V>[] children;

        /**
         * The total entry count of this page and all children.
         */
        private long totalCount;

        NonLeaf(MVMap<K, V> map) {
            super(map);
        }

        NonLeaf(MVMap<K, V> map,
                NonLeaf<K, V> source,
                PageReference<K, V>[] children,
                long totalCount) {
            super(map, source);
            this.children = children;
            this.totalCount = totalCount;
        }

        NonLeaf(MVMap<K, V> map, K[] keys,
                PageReference<K, V>[] children,
                long totalCount) {
            super(map, keys);
            this.children = children;
            this.totalCount = totalCount;
        }

        @Override
        public int getNodeType() {
            return DataUtils.PAGE_TYPE_NODE;
        }

        @Override
        public Page<K, V> copy(MVMap<K, V> mvMap, boolean eraseChildrenRefs) {
            return eraseChildrenRefs ?
                    new IncompleteNonLeaf<>(mvMap, this) :
                    new NonLeaf<>(mvMap, this, children, totalCount);
        }

        @Override
        public Page<K, V> getChildPage(int index) {
            PageReference<K, V> pageReference = children[index];
            Page<K, V> page = pageReference.page;
            if (page == null) {
                page = mvMap.readPage(pageReference.position);

                assert pageReference.position == page.position;
                assert pageReference.count == page.getTotalCount();
            }
            return page;
        }

        @Override
        public long getChildPagePos(int index) {
            return children[index].getPosition();
        }

        @Override
        public V getValue(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Page<K, V> split(int at) {
            assert !isSaved();
            int b = getKeyCount() - at;
            K[] bKeys = splitKeys(at, b - 1);
            PageReference<K, V>[] aChildren = createRefStorage(at + 1);
            PageReference<K, V>[] bChildren = createRefStorage(b);
            System.arraycopy(children, 0, aChildren, 0, at + 1);
            System.arraycopy(children, at + 1, bChildren, 0, b);
            children = aChildren;

            long t = 0;
            for (PageReference<K, V> x : aChildren) {
                t += x.count;
            }
            totalCount = t;
            t = 0;
            for (PageReference<K, V> x : bChildren) {
                t += x.count;
            }
            Page<K, V> newPage = createNoLeaf(mvMap, bKeys, bChildren, t, 0);
            if (isPersistent()) {
                recalculateMemory();
            }
            return newPage;
        }

        @Override
        public void expand(int keyCount, Object[] extraKeys, Object[] extraValues) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTotalCount() {
            assert !isComplete() || totalCount == calculateTotalCount() :
                    "Total count: " + totalCount + " != " + calculateTotalCount();
            return totalCount;
        }

        private long calculateTotalCount() {
            long totalCount = 0;
            int keyCount = getKeyCount();
            for (int i = 0; i <= keyCount; i++) {
                totalCount += children[i].count;
            }
            return totalCount;
        }

        void recalculateTotalCount() {
            totalCount = calculateTotalCount();
        }

        @Override
        long getCounts(int index) {
            return children[index].count;
        }

        @Override
        public void setChild(int index, Page<K, V> childPage) {
            PageReference<K, V> child = children[index];
            if (childPage != child.page || childPage.getPosition() != child.getPosition()) {
                totalCount += childPage.getTotalCount() - child.count;
                children = children.clone();
                children[index] = new PageReference<>(childPage);
            }
        }

        @Override
        public V setValue(int index, V value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void insertLeaf(int index, K key, V value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void insertNode(int index, K key, Page<K, V> childPage) {
            int childCount = getRawChildPageCount();

            insertKey(index, key);

            PageReference<K, V>[] newChildren = createRefStorage(childCount + 1);
            DataUtils.copyWithGap(children, newChildren, childCount, index);
            children = newChildren;

            children[index] = new PageReference<>(childPage);

            totalCount += childPage.getTotalCount();

            if (isPersistent()) {
                addMemory(MEMORY_POINTER + PAGE_MEMORY_CHILD);
            }
        }

        @Override
        public void remove(int index) {
            int childCount = getRawChildPageCount();
            super.remove(index);
            if (isPersistent()) {
                if (mvMap.isMemoryEstimationAllowed()) {
                    addMemory(-getMemory() / childCount);
                } else {
                    addMemory(-MEMORY_POINTER - PAGE_MEMORY_CHILD);
                }
            }
            totalCount -= children[index].count;
            PageReference<K, V>[] newChildren = createRefStorage(childCount - 1);
            DataUtils.copyExcept(children, newChildren, childCount, index);
            children = newChildren;
        }

        @Override
        public int removeAllRecursive(long version) {
            int unsavedMemory = removePage(version);
            if (isPersistent()) {
                for (int i = 0, size = mvMap.getChildPageCount(this); i < size; i++) {
                    PageReference<K, V> pageReference = children[i];
                    Page<K, V> page = pageReference.page;
                    if (page != null) {
                        unsavedMemory += page.removeAllRecursive(version);
                    } else {
                        long pagePos = pageReference.getPosition();
                        assert DataUtils.isPageSaved(pagePos);
                        if (DataUtils.isLeafPosition(pagePos)) {
                            mvMap.mvStore.accountForRemovedPage(pagePos, version, mvMap.isSingleWriter(), -1);
                        } else {
                            unsavedMemory += mvMap.readPage(pagePos).removeAllRecursive(version);
                        }
                    }
                }
            }
            return unsavedMemory;
        }

        @Override
        public CursorPos<K, V> getPrependCursorPos(CursorPos<K, V> cursorPos) {
            Page<K, V> childPage = getChildPage(0);
            return childPage.getPrependCursorPos(new CursorPos<>(this, 0, cursorPos));
        }

        @Override
        public CursorPos<K, V> getAppendCursorPos(CursorPos<K, V> cursorPos) {
            int keyCount = getKeyCount();
            Page<K, V> childPage = getChildPage(keyCount);
            return childPage.getAppendCursorPos(new CursorPos<>(this, keyCount, cursorPos));
        }

        @Override
        protected void readPayLoad(ByteBuffer byteBuffer) {
            int keyCount = getKeyCount();
            children = createRefStorage(keyCount + 1);

            long[] positions = new long[keyCount + 1];
            for (int a = 0; a <= keyCount; a++) {
                positions[a] = byteBuffer.getLong();
            }

            long total = 0;

            for (int a = 0; a <= keyCount; a++) {
                long count = DataUtils.readVarLong(byteBuffer);

                long position = positions[a];

                assert position == 0 ? count == 0 : count >= 0;

                total += count;

                children[a] = position == 0 ? PageReference.empty() : new PageReference<>(position, count);
            }

            totalCount = total;
        }

        @Override
        protected void writeValues(WriteBuffer buff) {
        }

        @Override
        protected void writeChildren(WriteBuffer buff, boolean withCounts) {
            int keyCount = getKeyCount();
            for (int i = 0; i <= keyCount; i++) {
                buff.putLong(children[i].getPosition());
            }
            if (withCounts) {
                for (int i = 0; i <= keyCount; i++) {
                    buff.putVarLong(children[i].count);
                }
            }
        }

        @Override
        void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff, List<Long> toc) {
            if (isSaved()) {
                return;
            }

            int patch = write(chunk, buff, toc);
            writeChildrenRecursive(chunk, buff, toc);
            int old = buff.position();
            buff.position(patch);
            writeChildren(buff, false);
            buff.position(old);
        }

        void writeChildrenRecursive(Chunk chunk, WriteBuffer buff, List<Long> toc) {
            int len = getRawChildPageCount();
            for (int i = 0; i < len; i++) {
                PageReference<K, V> pageReference = children[i];
                Page<K, V> p = pageReference.page;
                if (p != null) {
                    p.writeUnsavedRecursive(chunk, buff, toc);
                    pageReference.resetPos();
                }
            }
        }

        @Override
        void releaseSavedPages() {
            int len = getRawChildPageCount();
            for (int i = 0; i < len; i++) {
                children[i].clearPageReference();
            }
        }

        @Override
        public int getRawChildPageCount() {
            return getKeyCount() + 1;
        }

        @Override
        protected int calculateMemory() {
            return super.calculateMemory() + PAGE_NODE_MEMORY +
                    getRawChildPageCount() * (MEMORY_POINTER + PAGE_MEMORY_CHILD);
        }

        @Override
        public void dump(StringBuilder buff) {
            super.dump(buff);
            int keyCount = getKeyCount();
            for (int i = 0; i <= keyCount; i++) {
                if (i > 0) {
                    buff.append(" ");
                }
                buff.append("[").append(Long.toHexString(children[i].getPosition())).append("]");
                if (i < keyCount) {
                    buff.append(" ").append(getKey(i));
                }
            }
        }
    }

    private static class IncompleteNonLeaf<K, V> extends NonLeaf<K, V> {

        private boolean complete;

        IncompleteNonLeaf(MVMap<K, V> map, NonLeaf<K, V> source) {
            super(map, source, constructEmptyPageRefs(source.getRawChildPageCount()), source.getTotalCount());
        }

        private static <K, V> PageReference<K, V>[] constructEmptyPageRefs(int size) {
            // replace child pages with empty pages
            PageReference<K, V>[] children = createRefStorage(size);
            Arrays.fill(children, PageReference.empty());
            return children;
        }

        @Override
        void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff, List<Long> toc) {
            if (complete) {
                super.writeUnsavedRecursive(chunk, buff, toc);
            } else if (!isSaved()) {
                writeChildrenRecursive(chunk, buff, toc);
            }
        }

        @Override
        public boolean isComplete() {
            return complete;
        }

        @Override
        public void setComplete() {
            recalculateTotalCount();
            complete = true;
        }

        @Override
        public void dump(StringBuilder buff) {
            super.dump(buff);
            buff.append(", complete:").append(complete);
        }

    }

    private static class Leaf<K, V> extends Page<K, V> {
        /**
         * The storage for values.
         */
        private V[] values;

        Leaf(MVMap<K, V> map) {
            super(map);
        }

        private Leaf(MVMap<K, V> map, Leaf<K, V> source) {
            super(map, source);
            this.values = source.values;
        }

        Leaf(MVMap<K, V> map, K[] keys, V[] values) {
            super(map, keys);
            this.values = values;
        }

        @Override
        public int getNodeType() {
            return PAGE_TYPE_LEAF;
        }

        @Override
        public Page<K, V> copy(MVMap<K, V> mvMap, boolean eraseChildrenRefs) {
            return new Leaf<>(mvMap, this);
        }

        @Override
        public Page<K, V> getChildPage(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getChildPagePos(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public V getValue(int index) {
            return values == null ? null : values[index];
        }

        @Override
        public Page<K, V> split(int at) {
            assert !isSaved();
            int b = getKeyCount() - at;
            K[] bKeys = splitKeys(at, b);
            V[] bValues = createValueStorage(b);
            if (values != null) {
                V[] aValues = createValueStorage(at);
                System.arraycopy(values, 0, aValues, 0, at);
                System.arraycopy(values, at, bValues, 0, b);
                values = aValues;
            }
            Page<K, V> newPage = createLeaf(mvMap, bKeys, bValues, 0);
            if (isPersistent()) {
                recalculateMemory();
            }
            return newPage;
        }

        @Override
        public void expand(int extraKeyCount, K[] extraKeys, V[] extraValues) {
            int keyCount = getKeyCount();
            expandKeys(extraKeyCount, extraKeys);
            if (values != null) {
                V[] newValues = createValueStorage(keyCount + extraKeyCount);
                System.arraycopy(values, 0, newValues, 0, keyCount);
                System.arraycopy(extraValues, 0, newValues, keyCount, extraKeyCount);
                values = newValues;
            }
            if (isPersistent()) {
                recalculateMemory();
            }
        }

        @Override
        public long getTotalCount() {
            return getKeyCount();
        }

        @Override
        long getCounts(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setChild(int index, Page<K, V> childPage) {
            throw new UnsupportedOperationException();
        }

        @Override
        public V setValue(int index, V value) {
            values = values.clone();
            V old = setValueInternal(index, value);
            if (isPersistent()) {
                if (!mvMap.isMemoryEstimationAllowed()) {
                    addMemory(mvMap.evaluateMemoryForValue(value) - mvMap.evaluateMemoryForValue(old));
                }
            }
            return old;
        }

        private V setValueInternal(int index, V value) {
            V old = values[index];
            values[index] = value;
            return old;
        }

        @Override
        public void insertLeaf(int index, K key, V value) {
            int keyCount = getKeyCount();
            insertKey(index, key);

            if (values == null) {
                return;
            }

            V[] newValues = createValueStorage(keyCount + 1);
            DataUtils.copyWithGap(values, newValues, keyCount, index);
            values = newValues;

            setValueInternal(index, value);

            if (isPersistent()) {
                addMemory(MEMORY_POINTER + mvMap.evaluateMemoryForValue(value));
            }
        }

        @Override
        public void insertNode(int index, K key, Page<K, V> childPage) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void remove(int index) {
            int keyCount = getKeyCount();

            super.remove(index);
            if (values != null) {
                if (isPersistent()) {
                    if (mvMap.isMemoryEstimationAllowed()) {
                        addMemory(-getMemory() / keyCount);
                    } else {
                        V old = getValue(index);
                        addMemory(-MEMORY_POINTER - mvMap.evaluateMemoryForValue(old));
                    }
                }
                V[] newValues = createValueStorage(keyCount - 1);
                DataUtils.copyExcept(values, newValues, keyCount, index);
                values = newValues;
            }
        }

        @Override
        public int removeAllRecursive(long version) {
            return removePage(version);
        }

        @Override
        public CursorPos<K, V> getPrependCursorPos(CursorPos<K, V> cursorPos) {
            return new CursorPos<>(this, -1, cursorPos);
        }

        @Override
        public CursorPos<K, V> getAppendCursorPos(CursorPos<K, V> cursorPos) {
            int keyCount = getKeyCount();
            return new CursorPos<>(this, ~keyCount, cursorPos);
        }

        @Override
        protected void readPayLoad(ByteBuffer byteBuffer) {
            int keyCount = getKeyCount();
            values = createValueStorage(keyCount);
            mvMap.getValueType().read(byteBuffer, values, getKeyCount());
        }

        @Override
        protected void writeValues(WriteBuffer buff) {
            mvMap.getValueType().write(buff, values, getKeyCount());
        }

        @Override
        protected void writeChildren(WriteBuffer buff, boolean withCounts) {
        }

        @Override
        void writeUnsavedRecursive(Chunk chunk, WriteBuffer writeBuffer, List<Long> toc) {
            if (isSaved()) {
                return;
            }

            write(chunk, writeBuffer, toc);
        }

        @Override
        void releaseSavedPages() {
        }

        @Override
        public int getRawChildPageCount() {
            return 0;
        }

        @Override
        protected int calculateMemory() {
//*
            return super.calculateMemory() + PAGE_LEAF_MEMORY +
                    (values == null ? 0 : mvMap.evaluateMemoryForValues(values, getKeyCount()));
/*/
            int keyCount = getKeyCount();
            int mem = super.calculateMemory() + PAGE_LEAF_MEMORY + keyCount * MEMORY_POINTER;
            DataType<V> valueType = map.getValueType();
            for (int i = 0; i < keyCount; i++) {
                mem += getMemory(valueType, values[i]);
            }
            return mem;
//*/
        }

        @Override
        public void dump(StringBuilder buff) {
            super.dump(buff);
            int keyCount = getKeyCount();
            for (int i = 0; i < keyCount; i++) {
                if (i > 0) {
                    buff.append(" ");
                }
                buff.append(getKey(i));
                if (values != null) {
                    buff.append(':');
                    buff.append(getValue(i));
                }
            }
        }
    }
}
