/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import org.h2.store.fs.FileBase;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FilePathWrapper;

/**
 * A file with a read cache.
 */
public class FilePathCache extends FilePathWrapper {

    public static final FilePathCache INSTANCE = new FilePathCache();

    static {
        FilePath.register(INSTANCE);
    }

    public static FileChannel wrap(FileChannel f) {
        return new FileCache(f);
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileCache(getFilePath().open(mode));
    }

    @Override
    public String getScheme() {
        return "cache";
    }

    public static class FileCache extends FileBase {

        private static final int CACHE_BLOCK_SIZE = 4 * 1024;
        private final FileChannel fileChannel;

        private final CacheLongKeyLIRS<ByteBuffer> cache;

        {
            CacheLongKeyLIRS.Config config = new CacheLongKeyLIRS.Config();
            // 1 MB cache size
            config.maxMemory = 1024 * 1024;
            cache = new CacheLongKeyLIRS<>(config);
        }

        FileCache(FileChannel fileChannel) {
            this.fileChannel = fileChannel;
        }

        @Override
        protected void implCloseChannel() throws IOException {
            fileChannel.close();
        }

        @Override
        public FileChannel position(long newPosition) throws IOException {
            fileChannel.position(newPosition);
            return this;
        }

        @Override
        public long position() throws IOException {
            return fileChannel.position();
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return fileChannel.read(dst);
        }

        @Override
        public synchronized int read(ByteBuffer dest, long position) throws IOException {
            long cachePos = getCachePos(position);
            int off = (int) (position - cachePos);
            int len = CACHE_BLOCK_SIZE - off;
            len = Math.min(len, dest.remaining());

            ByteBuffer byteBuffer = cache.get(cachePos);
            if (byteBuffer == null) {
                byteBuffer = ByteBuffer.allocate(CACHE_BLOCK_SIZE);
                long pos = cachePos;

                while (true) {
                    int read = fileChannel.read(byteBuffer, pos);
                    if (read <= 0) {
                        break;
                    }
                    if (byteBuffer.remaining() == 0) {
                        break;
                    }
                    pos += read;
                }

                int read = byteBuffer.position();
                if (read == CACHE_BLOCK_SIZE) {
                    cache.put(cachePos, byteBuffer, CACHE_BLOCK_SIZE);
                } else {
                    if (read <= 0) {
                        return -1;
                    }
                    len = Math.min(len, read - off);
                }
            }

            dest.put(byteBuffer.array(), off, len);

            return len == 0 ? -1 : len;
        }

        private static long getCachePos(long pos) {
            return (pos / CACHE_BLOCK_SIZE) * CACHE_BLOCK_SIZE;
        }

        @Override
        public long size() throws IOException {
            return fileChannel.size();
        }

        @Override
        public synchronized FileChannel truncate(long newSize) throws IOException {
            cache.clear();
            fileChannel.truncate(newSize);
            return this;
        }

        @Override
        public synchronized int write(ByteBuffer src, long position) throws IOException {
            clearCache(src, position);
            return fileChannel.write(src, position);
        }

        @Override
        public synchronized int write(ByteBuffer src) throws IOException {
            clearCache(src, position());
            return fileChannel.write(src);
        }

        private void clearCache(ByteBuffer src, long position) {
            if (cache.size() > 0) {
                int len = src.remaining();
                long p = getCachePos(position);
                while (len > 0) {
                    cache.remove(p);
                    p += CACHE_BLOCK_SIZE;
                    len -= CACHE_BLOCK_SIZE;
                }
            }
        }

        @Override
        public void force(boolean metaData) throws IOException {
            fileChannel.force(metaData);
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return fileChannel.tryLock(position, size, shared);
        }

        @Override
        public String toString() {
            return "cache:" + fileChannel.toString();
        }
    }
}
