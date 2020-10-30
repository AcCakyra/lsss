package com.accakyra.lsss.lsm;

import com.accakyra.lsss.DAO;
import com.accakyra.lsss.lsm.store.LSMIterator;
import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.io.TableReader;
import com.accakyra.lsss.lsm.io.TableWriter;
import com.accakyra.lsss.lsm.store.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMTree implements DAO {

    private Memtable memtable;
    private Memtable immtable;
    private final List<SST> ssts;
    private final TableWriter tableWriter;
    private final TableReader tableReader;
    private final ReadWriteLock mutex;
    private final MetaData metaData;

    public LSMTree(File data) {
        mutex = new ReentrantReadWriteLock();
        metaData = new MetaData(data);
        tableWriter = new TableWriter(metaData, data);
        tableReader = new TableReader(data);
        ssts = tableReader.readSSTs(metaData.getSstGeneration());
        memtable = new Memtable();
    }

    public void upsert(ByteBuffer key, ByteBuffer value) {
        mutex.writeLock().lock();

        if (!memtable.canStore(key, value)) {
            switchToNewMemtable();
        }

        memtable.upsert(key, value);
        mutex.writeLock().unlock();
    }

    public ByteBuffer get(ByteBuffer key) throws NoSuchElementException {
        mutex.readLock().lock();
        Record record = memtable.get(key);
        if (record == null) {
            if (immtable != null) record = immtable.get(key);
            if (record == null) {
                for (SST sst : ssts) {
                    record = sst.get(key);
                    if (record != null) break;
                }
            }
        }
        mutex.readLock().unlock();
        if (record == null || record.getValue().equals(LSMProperties.TOMBSTONE)) {
            throw new NoSuchElementException();
        }
        return record.getValue();
    }

    public void delete(ByteBuffer key) {
        upsert(key, LSMProperties.TOMBSTONE);
    }

    public Iterator<Record> iterator() {
        mutex.readLock().lock();
        List<Iterator<Record>> iterators = new ArrayList<>();

        iterators.add(memtable.iterator());
        if (immtable != null) iterators.add(immtable.iterator());
        for (SST sst : ssts) iterators.add(sst.iterator());

        mutex.readLock().unlock();
        return new LSMIterator(iterators, mutex);
    }

    public Iterator<Record> iterator(ByteBuffer from) {
        mutex.readLock().lock();
        List<Iterator<Record>> iterators = new ArrayList<>();

        iterators.add(memtable.iterator(from));
        if (immtable != null) iterators.add(immtable.iterator(from));
        for (SST sst : ssts) iterators.add(sst.iterator(from));

        mutex.readLock().unlock();
        return new LSMIterator(iterators, mutex);
    }

    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        mutex.readLock().lock();
        List<Iterator<Record>> iterators = new ArrayList<>();

        iterators.add(memtable.iterator(from, to));
        if (immtable != null) iterators.add(immtable.iterator(from, to));
        for (SST sst : ssts) iterators.add(sst.iterator(from, to));

        mutex.readLock().unlock();
        return new LSMIterator(iterators, mutex);
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
        memtable = new Memtable();
    }

    private void scheduleMemtableFlushing(Memtable memtable) {
        // Overall size of key in index file is: 12 + key size
        // 4 bytes for storing length of key
        // 4 bytes for storing offset in sst file for value of this key
        // 4 bytes for storing length of value
        int indexBufferSize = memtable.getKeysCapacity() + 12 * memtable.getUniqueKeysCount();

        ByteBuffer indexBuffer = ByteBuffer.allocate(indexBufferSize);
        ByteBuffer sstBuffer = ByteBuffer.allocate(memtable.getTotalBytesCapacity());
        NavigableMap<ByteBuffer, KeyInfo> indexKeys = new TreeMap<>();

        int valueOffset = 0;
        for (Record record : memtable) {
            byte[] key = record.getKey().array();
            byte[] value = record.getValue().array();

            sstBuffer.put(key);
            sstBuffer.put(value);

            valueOffset += key.length;

            indexBuffer.putInt(key.length);
            indexBuffer.put(key);
            indexBuffer.putInt(valueOffset);
            indexBuffer.putInt(value.length);
            indexKeys.put(record.getKey(), new KeyInfo(valueOffset, value.length));

            valueOffset += value.length;
        }

        sstBuffer.flip();
        indexBuffer.flip();

        Index index = new Index(indexKeys, metaData.getAndIncrementIndexGeneration());
        SST sst = new SST(index, tableReader);
        ssts.add(sst);

        tableWriter.scheduleMemtableFlushing(sstBuffer, indexBuffer);
    }
}
