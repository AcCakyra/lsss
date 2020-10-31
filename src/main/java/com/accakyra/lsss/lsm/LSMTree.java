package com.accakyra.lsss.lsm;

import com.accakyra.lsss.DAO;
import com.accakyra.lsss.lsm.store.LSMIterator;
import com.accakyra.lsss.lsm.store.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMTree implements DAO {

    private Memtable memtable;
    private Memtable immtable;
    private final SSTables sstables;
    private final ReadWriteLock mutex;

    public LSMTree(File data) {
        mutex = new ReentrantReadWriteLock();
        memtable = new Memtable();
        sstables = new SSTables(data);
    }

    public void upsert(ByteBuffer key, ByteBuffer value) {
        mutex.writeLock().lock();
        if (!memtable.canStore(key, value)) {
            createNewMemtable();
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
                for (Resource sst : sstables) {
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
        for (Resource sst : sstables) iterators.add(sst.iterator());

        mutex.readLock().unlock();
        return new LSMIterator(iterators, mutex);
    }

    public Iterator<Record> iterator(ByteBuffer from) {
        mutex.readLock().lock();
        List<Iterator<Record>> iterators = new ArrayList<>();

        iterators.add(memtable.iterator(from));
        if (immtable != null) iterators.add(immtable.iterator(from));
        for (Resource sst : sstables) iterators.add(sst.iterator(from));

        mutex.readLock().unlock();
        return new LSMIterator(iterators, mutex);
    }

    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        mutex.readLock().lock();
        List<Iterator<Record>> iterators = new ArrayList<>();

        iterators.add(memtable.iterator(from, to));
        if (immtable != null) iterators.add(immtable.iterator(from, to));
        for (Resource sst : sstables) iterators.add(sst.iterator(from, to));

        mutex.readLock().unlock();
        return new LSMIterator(iterators, mutex);
    }

    public void close() {
        mutex.writeLock().lock();
        sstables.writeMemtable(memtable);
        sstables.close();
        mutex.writeLock().unlock();
    }

    private void createNewMemtable() {
        sstables.writeMemtable(memtable);
        immtable = memtable;
        memtable = new Memtable();
    }
}
