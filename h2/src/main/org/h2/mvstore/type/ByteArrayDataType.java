/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import java.nio.ByteBuffer;

/**
 * Class ByteArrayDataType.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class ByteArrayDataType extends BasicDataType<byte[]>
{
    public static final ByteArrayDataType INSTANCE = new ByteArrayDataType();

    private ByteArrayDataType() {}

    @Override
    public int getMemory(byte[] data) {
        return data.length;
    }

    @Override
    public void write(WriteBuffer writeBuffer, byte[] data) {
        writeBuffer.putVarInt(data.length);
        writeBuffer.put(data);
    }

    @Override
    public byte[] read(ByteBuffer byteBuffer) {
        int size = DataUtils.readVarInt(byteBuffer);
        byte[] data = new byte[size];
        byteBuffer.get(data);
        return data;
    }

    @Override
    public byte[][] createStorage(int size) {
        return new byte[size][];
    }
}
