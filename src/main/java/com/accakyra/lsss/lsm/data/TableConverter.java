package com.accakyra.lsss.lsm.data;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.data.memory.Memtable;
import com.accakyra.lsss.lsm.data.persistent.sst.KeyInfo;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;
import com.accakyra.lsss.lsm.data.persistent.io.Table;
import com.accakyra.lsss.lsm.util.FileNameUtil;
import com.accakyra.lsss.lsm.util.iterators.IteratorsUtil;

import java.nio.ByteBuffer;
import java.nio.file.Path;
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
        int indexSize = 4 + memtable.getKeysCapacity() + keysInfoSize;
        int memtableSize = memtable.getTotalBytesCapacity();

        ByteBuffer indexBuffer = ByteBuffer.allocateDirect(indexSize);
        ByteBuffer sstBuffer = ByteBuffer.allocateDirect(memtableSize);

        indexBuffer.putInt(level);

        Iterator<Record> memtableIterator = IteratorsUtil.distinctIterator(memtable.iterator());

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

    public static NavigableMap<ByteBuffer, KeyInfo> parseIndexBuffer(ByteBuffer buffer, boolean sparse) {
        int indexSparseStep = 10;
        int keysCounter = 0;

        NavigableMap<ByteBuffer, KeyInfo> keys = new TreeMap<>();

        int keyOffset = 0;
        while (buffer.hasRemaining()) {
            int keySize = buffer.getInt();
            byte[] key = new byte[keySize];
            buffer.get(key);
            ByteBuffer keyBuffer = ByteBuffer.wrap(key);
            int valueOffset = buffer.getInt();
            int valueSize = buffer.getInt();
            if (sparse) {
                if (keysCounter++ % indexSparseStep == 0 || !buffer.hasRemaining()) {
                    if (keySize > 4) {
                        keyBuffer = ByteBuffer.wrap(keyBuffer.limit(4).array());
                    }
                    keys.put(keyBuffer, new KeyInfo(keyOffset, valueOffset, keySize, valueSize));
                }
            } else {
                keys.put(keyBuffer, new KeyInfo(keyOffset, valueOffset, keySize, valueSize));
            }
            keyOffset += 12;
            keyOffset += keySize;
        }
        return keys;
    }
}
