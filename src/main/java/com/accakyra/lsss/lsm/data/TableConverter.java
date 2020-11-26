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

    public static SST convertMemtableToSST(Memtable memtable, int tableId, int level, Path storagePath) {
        NavigableMap<ByteBuffer, KeyInfo> index = new TreeMap<>();

        int valueOffset = 0;
        Iterator<Record> memtableIterator = IteratorsUtil.distinctIterator(memtable.iterator());
        while (memtableIterator.hasNext()) {
            Record record = memtableIterator.next();
            ByteBuffer key = record.getKey().asReadOnlyBuffer();
            ByteBuffer value = record.getValue().asReadOnlyBuffer();
            valueOffset += key.capacity();
            index.put(key, new KeyInfo(valueOffset, value.capacity()));
            valueOffset += value.capacity();
        }

        Path fileName = FileNameUtil.buildSSTableFileName(storagePath, tableId);
        return new SST(index, tableId, fileName, level);
    }

    public static NavigableMap<ByteBuffer, KeyInfo> parseIndexBuffer(ByteBuffer buffer) {
        NavigableMap<ByteBuffer, KeyInfo> keys = new TreeMap<>();
        while (buffer.hasRemaining()) {
            int keySize = buffer.getInt();
            byte[] key = new byte[keySize];
            buffer.get(key);
            ByteBuffer keyBuffer = ByteBuffer.wrap(key);
            int offset = buffer.getInt();
            int valueSize = buffer.getInt();
            keys.put(keyBuffer, new KeyInfo(offset, valueSize));
        }
        return keys;
    }
}
