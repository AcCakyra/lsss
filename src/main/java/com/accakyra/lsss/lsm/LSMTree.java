package com.accakyra.lsss.lsm;

import com.accakyra.lsss.DAO;
import com.accakyra.lsss.lsm.io.TableReader;
import com.accakyra.lsss.lsm.io.TableWriter;
import com.accakyra.lsss.lsm.store.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMTree implements DAO {

    private MemTable memtable;
    private MemTable immtable;
    private List<Index> indexes;
    private final TableWriter tableWriter;
    private final TableReader tableReader;
    private final ReadWriteLock mutex;
    private int memtableByteCount;
    private final MetaData metaData;

    public LSMTree(File data) {
        mutex = new ReentrantReadWriteLock();
        metaData = new MetaData(data);
        tableWriter = new TableWriter(metaData, data);
        tableReader = new TableReader(data);
        indexes = tableReader.readIndexes(metaData.getIndexGeneration());
        memtable = new MemTable();
    }

    public void upsert(ByteBuffer key, ByteBuffer value) {
        mutex.writeLock().lock();

        int newEntrySize = key.capacity() + value.capacity();
        if (memtableByteCount + newEntrySize > LSMProperties.MEMTABLE_THRESHOLD) {
            switchToNewMemtable();
        }

        memtableByteCount += newEntrySize;
        memtable.upsert(key, value);
        mutex.writeLock().unlock();
    }

    public ByteBuffer get(ByteBuffer key) throws NoSuchElementException {
        mutex.readLock().lock();
        ByteBuffer value = memtable.get(key);
        if (value == null) {
            if (immtable != null) value = immtable.get(key);
            if (value == null) {
                for (Index index : indexes) {
                    Key indexKey = index.getKey(key);
                    if (indexKey != null) {
                        value = tableReader.get(indexKey, index.getGeneration());
                    }
                }
            }
        }
        mutex.readLock().unlock();
        if (value == null || value.equals(LSMProperties.TOMBSTONE)) {
            throw new NoSuchElementException();
        }
        return value;
    }

    public void delete(ByteBuffer key) {
        upsert(key, LSMProperties.TOMBSTONE);
    }

    public void close() {
        mutex.writeLock().lock();
        switchToNewMemtable();
        tableWriter.close();
        metaData.close();
        mutex.writeLock().unlock();
    }

    private void switchToNewMemtable() {
        scheduleMemtableFlushing(memtable);
        immtable = memtable;
        memtable = new MemTable();
        memtableByteCount = 0;
    }

    private void scheduleMemtableFlushing(MemTable memtable) {
        // Overall size of key in index file is: 12 + key size
        // 4 bytes for storing length of key
        // 4 bytes for storing offset in sst file for value of this key
        // 4 bytes for storing length of value
        int indexBufferSize = memtable.getKeysCapacity() + 12 * memtable.getSize();

        ByteBuffer indexBuffer = ByteBuffer.allocate(indexBufferSize);
        ByteBuffer sstBuffer = ByteBuffer.allocate(LSMProperties.MEMTABLE_THRESHOLD);
        NavigableSet<Key> indexKeys = new TreeSet<>();

        int valueOffset = 0;
        Iterator<Map.Entry<ByteBuffer, ByteBuffer>> memtableIterator = memtable.getIterator();
        while (memtableIterator.hasNext()) {
            Map.Entry<ByteBuffer, ByteBuffer> record = memtableIterator.next();

            byte[] key = record.getKey().array();
            byte[] value = record.getValue().array();

            sstBuffer.put(key);
            sstBuffer.put(value);

            valueOffset += key.length;

            indexBuffer.putInt(key.length);
            indexBuffer.put(key);
            indexBuffer.putInt(valueOffset);
            indexBuffer.putInt(value.length);
            indexKeys.add(new Key(record.getKey(), valueOffset, value.length));

            valueOffset += value.length;
        }

        sstBuffer.flip();
        indexBuffer.flip();

        Index index = new Index(indexKeys, metaData.getAndIncrementIndexGeneration());
        indexes.add(index);
        tableWriter.scheduleMemtableFlushing(sstBuffer, indexBuffer);
    }
}
