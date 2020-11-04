package com.accakyra.lsss.lsm;

import com.accakyra.lsss.DAO;
import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.storage.Memtable;
import com.accakyra.lsss.lsm.storage.Resource;
import com.accakyra.lsss.lsm.storage.SST;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMTree implements DAO {
    /**
     * Special value for deleted keys.
     */
    public static ByteBuffer TOMBSTONE = ByteBuffer.wrap("TTTT".getBytes());

    private Memtable memtable;
    private Memtable immtable;
    private final TableManager tableManager;
    private final List<SST> sstables;
    private ReadWriteLock mutex;

    public LSMTree(File data) {
        memtable = new Memtable();
        immtable = new Memtable();
        tableManager = new TableManager(data);
        sstables = tableManager.readSSTs();
        mutex = new ReentrantReadWriteLock();
    }

    @Override
    public synchronized void upsert(ByteBuffer key, ByteBuffer value) {
        if (!memtable.canStore(key, value)) {
            createNewMemtable();
        }
        memtable.upsert(key, value);
    }

    @Override
    public ByteBuffer get(ByteBuffer key) throws NoSuchElementException {
        mutex.readLock().lock();
        Record record = memtable.get(key);
        if (record == null) {
            record = immtable.get(key);
            if (record == null) {
                record = sstables.stream()
                        .filter(sst -> sst.contains(key))
                        .findFirst()
                        .map(sst -> sst.get(key))
                        .orElse(null);
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
        List<Iterator<Record>> iterators = iterators();
        return new LSMIterator(iterators);
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from) {
        List<Iterator<Record>> iterators = iterators(from);
        return new LSMIterator(iterators);
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        List<Iterator<Record>> iterators = iterators(from, to);
        return new LSMIterator(iterators);
    }

    private ByteBuffer extractValue(Record record) {
        if (record == null || record.getValue().equals(TOMBSTONE)) throw new NoSuchElementException();
        else return record.getValue();
    }

    private List<Iterator<Record>> iterators() {
        List<Iterator<Record>> iterators = new ArrayList<>();

        mutex.readLock().lock();
        iterators.add(memtable.iterator());
        iterators.add(immtable.iterator());
        mutex.readLock().unlock();

        for (Resource sst : sstables) iterators.add(sst.iterator());
        return iterators;
    }

    private List<Iterator<Record>> iterators(ByteBuffer from) {
        List<Iterator<Record>> iterators = new ArrayList<>();

        mutex.readLock().lock();
        iterators.add(memtable.iterator(from));
        iterators.add(immtable.iterator(from));
        mutex.readLock().unlock();

        for (Resource sst : sstables) iterators.add(sst.iterator(from));
        return iterators;
    }

    private List<Iterator<Record>> iterators(ByteBuffer from, ByteBuffer to) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        mutex.readLock().lock();
        iterators.add(memtable.iterator(from, to));
        iterators.add(immtable.iterator(from, to));
        mutex.readLock().unlock();

        for (Resource sst : sstables) iterators.add(sst.iterator(from, to));
        return iterators;
    }

    @Override
    public void close() {
        createNewMemtable();
        tableManager.close();
    }

    private void createNewMemtable() {
        flushMemtable(memtable);
        mutex.writeLock().lock();
        immtable = memtable;
        memtable = new Memtable();
        mutex.writeLock().unlock();
    }

    private void flushMemtable(Memtable memtable) {
        SST sst = tableManager.writeMemtable(memtable);
        sstables.add(sst);
    }
}
