package com.accakyra.lsss.lsm.data;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.Config;
import com.accakyra.lsss.lsm.data.memory.Memtable;
import com.accakyra.lsss.lsm.data.persistent.sst.KeyInfo;
import com.accakyra.lsss.lsm.data.persistent.io.Table;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;
import com.accakyra.lsss.lsm.util.FileNameUtil;

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
            indexBuffer.putInt(valueOffset);
            indexBuffer.putInt(value.capacity());
            indexBuffer.put(key);

            valueOffset += value.capacity();
        }

        sstBuffer.flip();
        indexBuffer.flip();

        return new Table(indexBuffer, sstBuffer);
    }

    public static SST convertTableToSST(Table table, int tableId, int level,
                                        Path storage, int maxKeySize, int sparseStep) {
        NavigableMap<ByteBuffer, KeyInfo> index = TableConverter.parseIndexBufferSparse(
                table.getIndexBuffer().position(4), maxKeySize, sparseStep);
        Path sstFileName = FileNameUtil.buildSSTableFileName(storage, tableId);
        Path indexFileName = FileNameUtil.buildIndexFileName(storage, tableId);
        return new SST(index, tableId, sstFileName, indexFileName, level);
    }

    public static NavigableMap<ByteBuffer, KeyInfo> parseIndexBufferSparse(
            ByteBuffer indexBuffer, int maxKeySize, int sparseStep) {

        int keysCounter = 0;
        NavigableMap<ByteBuffer, KeyInfo> keys = new TreeMap<>();

        int keyOffset = 0;
        while (indexBuffer.hasRemaining()) {
            int keySize = indexBuffer.getInt();
            int valueOffset = indexBuffer.getInt();
            int valueSize = indexBuffer.getInt();

            if (keysCounter++ % sparseStep == 0 || indexBuffer.position() + keySize == indexBuffer.limit()) {
                ByteBuffer keyBuffer;
                if (keySize > maxKeySize) {
                    byte[] key = new byte[maxKeySize];
                    indexBuffer.get(key);
                    keyBuffer = ByteBuffer.wrap(key);
                    indexBuffer.position(indexBuffer.position() + keySize - maxKeySize);
                } else {
                    byte[] key = new byte[keySize];
                    indexBuffer.get(key);
                    keyBuffer = ByteBuffer.wrap(key);
                }
                keys.put(keyBuffer, new KeyInfo(keyOffset, valueOffset, keySize, valueSize));
            } else {
                indexBuffer.position(indexBuffer.position() + keySize);
            }

            keyOffset += 12;
            keyOffset += keySize;
        }
        return keys;
    }

    public static NavigableMap<ByteBuffer, KeyInfo> parseIndexBuffer(ByteBuffer indexBuffer) {
        NavigableMap<ByteBuffer, KeyInfo> keys = new TreeMap<>();

        int keyOffset = 0;
        while (indexBuffer.hasRemaining()) {
            int keySize = indexBuffer.getInt();
            int valueOffset = indexBuffer.getInt();
            int valueSize = indexBuffer.getInt();

            byte[] key = new byte[keySize];
            indexBuffer.get(key);
            ByteBuffer keyBuffer = ByteBuffer.wrap(key);
            keys.put(keyBuffer, new KeyInfo(keyOffset, valueOffset, keySize, valueSize));

            keyOffset += 12;
            keyOffset += keySize;
        }

        return keys;
    }

    public static KeyInfo extractInfoFromIndexBuffer(ByteBuffer indexBuffer, ByteBuffer key) {
        int keyOffset = 0;

        while (indexBuffer.hasRemaining()) {
            int keySize = indexBuffer.getInt();
            int valueOffset = indexBuffer.getInt();
            int valueSize = indexBuffer.getInt();
            byte[] byteKey = new byte[keySize];
            indexBuffer.get(byteKey);
            ByteBuffer keyBuffer = ByteBuffer.wrap(byteKey);

            if (keyBuffer.equals(key)) return new KeyInfo(keyOffset, valueOffset, keySize, valueSize);

            keyOffset += 12;
            keyOffset += keySize;
        }
        return null;
    }
}
