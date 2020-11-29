package com.accakyra.lsss.lsm.data;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.Config;
import com.accakyra.lsss.lsm.data.memory.Memtable;
import com.accakyra.lsss.lsm.data.persistent.sst.KeyInfo;
import com.accakyra.lsss.lsm.data.persistent.io.Table;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

public class TableConverter {

    public static Table convertMemtableToTable(Memtable memtable, int level) {
        // Overall size of index file is:
        // 4 bytes for level
        // + (4 bytes for storing length of key
        //    + 4 bytes for storing offset in sst file for value of this key
        //    + 4 bytes for storing length of value
        //   ) * count of keys
        // + size of all keys
        int keysInfoSize = (4 + 4 + 4) * memtable.getUniqueKeysCount();
        int indexSize = 4 + memtable.getKeysSize() + keysInfoSize;
        int memtableSize = memtable.getSize();

        ByteBuffer indexBuffer = ByteBuffer.allocateDirect(indexSize);
        ByteBuffer sstBuffer = ByteBuffer.allocateDirect(memtableSize);

        indexBuffer.putInt(level);

        Iterator<Record> memtableIterator = memtable.iterator();

        int valueOffset = 0;
        while (memtableIterator.hasNext()) {
            Record record = memtableIterator.next();
            ByteBuffer key = record.getKey().asReadOnlyBuffer();
            ByteBuffer value = record.getValue().asReadOnlyBuffer();

            sstBuffer.put(key);
            sstBuffer.put(value);

            key.flip();
            value.flip();

            valueOffset += key.capacity();
            indexBuffer.putInt(key.capacity());
            indexBuffer.put(key);
            indexBuffer.putInt(valueOffset);
            indexBuffer.putInt(value.capacity());
            valueOffset += value.capacity();
        }

        sstBuffer.flip();
        indexBuffer.flip();

        return new Table(indexBuffer, sstBuffer);
    }

    public static NavigableMap<ByteBuffer, KeyInfo> parseIndexBuffer(ByteBuffer indexBuffer) {
        return parseIndexBufferSparse(indexBuffer, Integer.MAX_VALUE, 1);
    }

    public static NavigableMap<ByteBuffer, KeyInfo> parseIndexBufferSparse(
            ByteBuffer indexBuffer, int maxKeySize, int sparseStep) {

        int keysCounter = 0;
        NavigableMap<ByteBuffer, KeyInfo> keys = new TreeMap<>();

        int keyOffset = 0;
        while (indexBuffer.hasRemaining()) {
            int keySize = indexBuffer.getInt();
            byte[] key = new byte[keySize];
            indexBuffer.get(key);
            ByteBuffer keyBuffer = ByteBuffer.wrap(key);
            int valueOffset = indexBuffer.getInt();
            int valueSize = indexBuffer.getInt();
            if (keysCounter++ % sparseStep == 0 || !indexBuffer.hasRemaining()) {
                if (keySize > maxKeySize) {
                    keyBuffer = ByteBuffer.wrap(keyBuffer.limit(maxKeySize).array());
                }
                keys.put(keyBuffer, new KeyInfo(keyOffset, valueOffset, keySize, valueSize));
            }
            keyOffset += 12;
            keyOffset += keySize;
        }
        return keys;
    }
}
