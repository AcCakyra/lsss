package com.accakyra.lsss.lsm;

import com.accakyra.lsss.DAO;
import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.storage.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMTree implements DAO {
    /**
     * Special value for deleted keys.
     */
    public static ByteBuffer TOMBSTONE = ByteBuffer.wrap("TTTT".getBytes());

    private Memtable memtable;
    private Memtable immtable;
    private final SSTables sstables;
    private final ReadWriteLock mutex;

    public LSMTree(File data) {
        mutex = new ReentrantReadWriteLock();
        memtable = new Memtable();
        sstables = new SSTables(data);
    }

    @Override
    public void upsert(ByteBuffer key, ByteBuffer value) {
        mutex.writeLock().lock();
        if (!memtable.canStore(key, value)) {
            createNewMemtable();
        }
        memtable.upsert(key, value);
        mutex.writeLock().unlock();
    }

    @Override
    public ByteBuffer get(ByteBuffer key) throws NoSuchElementException {
        mutex.readLock().lock();
        Record record = memtable.get(key);
        if (record == null) {
            if (immtable != null) {
                record = immtable.get(key);
            }
            if (record == null) {
                for (Resource sst : sstables) {
                    record = sst.get(key);
                    if (record != null) break;
                }
            }
        }
        mutex.readLock().unlock();
        return extractValue(record);
    }

    @Override
    public void delete(ByteBuffer key) {
        upsert(key, TOMBSTONE);
    }

    @Override
    public Iterator<Record> iterator() {
        mutex.readLock().lock();
        List<Iterator<Record>> iterators = iterators();
        mutex.readLock().unlock();
        return new LSMIterator(iterators, mutex);
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from) {
        mutex.readLock().lock();
        List<Iterator<Record>> iterators = iterators(from);
        mutex.readLock().unlock();
        return new LSMIterator(iterators, mutex);
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        mutex.readLock().lock();
        List<Iterator<Record>> iterators = iterators(from, to);
        mutex.readLock().unlock();
        return new LSMIterator(iterators, mutex);
    }

    private ByteBuffer extractValue(Record record) {
        if (record == null || record.getValue().equals(TOMBSTONE)) throw new NoSuchElementException();
        else return record.getValue();
    }

    private List<Iterator<Record>> iterators() {
        List<Iterator<Record>> iterators = new ArrayList<>();
        iterators.add(memtable.iterator());
        if (immtable != null) iterators.add(immtable.iterator());
        for (Resource sst : sstables) iterators.add(sst.iterator());
        return iterators;
    }

    private List<Iterator<Record>> iterators(ByteBuffer from) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        iterators.add(memtable.iterator(from));
        if (immtable != null) iterators.add(immtable.iterator(from));
        for (Resource sst : sstables) iterators.add(sst.iterator(from));
        return iterators;
    }

    private List<Iterator<Record>> iterators(ByteBuffer from, ByteBuffer to) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        iterators.add(memtable.iterator(from, to));
        if (immtable != null) iterators.add(immtable.iterator(from, to));
        for (Resource sst : sstables) iterators.add(sst.iterator(from, to));
        return iterators;
    }

    @Override
    public void close() {
        mutex.writeLock().lock();
        sstables.addMemtable(memtable);
        sstables.close();
        mutex.writeLock().unlock();
    }

    private void createNewMemtable() {
        sstables.addMemtable(memtable);
        immtable = memtable;
        memtable = new Memtable();
    }
}
