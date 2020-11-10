package com.accakyra.lsss.lsm;

import com.accakyra.lsss.DAO;
import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.storage.Memtable;
import com.accakyra.lsss.lsm.storage.Resource;
import com.accakyra.lsss.lsm.storage.Tables;

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
    private Tables tables;
    private ReadWriteLock memtableLock;

    public LSMTree(File data) {
        memtable = new Memtable();
        immtable = new Memtable();
        tables = new Tables(data);
        memtableLock = new ReentrantReadWriteLock();
    }

    @Override
    public void upsert(ByteBuffer key, ByteBuffer value) {
        memtableLock.writeLock().lock();
        if (!memtable.canStore(key, value)) {
            createNewMemtable();
        }
        memtable.upsert(key, value);
        memtableLock.writeLock().unlock();
    }

    @Override
    public ByteBuffer get(ByteBuffer key) throws NoSuchElementException {
        memtableLock.readLock().lock();
        Record record = memtable.get(key);
        if (record == null) {
            record = immtable.get(key);
            if (record == null) {
                record = tables.getLevels().stream()
                        .filter(level -> level.contains(key))
                        .findFirst()
                        .map(level -> level.get(key))
                        .orElse(null);
            }
        }
        memtableLock.readLock().unlock();
        return extractValue(record);
    }

    @Override
    public void delete(ByteBuffer key) {
        upsert(key, TOMBSTONE);
    }

    @Override
    public Iterator<Record> iterator() {
        List<Iterator<Record>> iterators = iterators();
        return new MergeIterator(iterators);
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from) {
        List<Iterator<Record>> iterators = iterators(from);
        return new MergeIterator(iterators);
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        List<Iterator<Record>> iterators = iterators(from, to);
        return new MergeIterator(iterators);
    }

    private ByteBuffer extractValue(Record record) {
        if (record == null || record.getValue().equals(TOMBSTONE)) throw new NoSuchElementException();
        else return record.getValue();
    }

    private List<Iterator<Record>> iterators() {
        List<Iterator<Record>> iterators = new ArrayList<>();

        memtableLock.readLock().lock();
        iterators.add(memtable.iterator());
        iterators.add(immtable.iterator());
        for (Resource level : tables.getLevels()) iterators.add(level.iterator());
        memtableLock.readLock().unlock();

        return iterators;
    }

    private List<Iterator<Record>> iterators(ByteBuffer from) {
        List<Iterator<Record>> iterators = new ArrayList<>();

        memtableLock.readLock().lock();
        iterators.add(memtable.iterator(from));
        iterators.add(immtable.iterator(from));
        for (Resource level : tables.getLevels()) iterators.add(level.iterator(from));
        memtableLock.readLock().unlock();

        return iterators;
    }

    private List<Iterator<Record>> iterators(ByteBuffer from, ByteBuffer to) {
        List<Iterator<Record>> iterators = new ArrayList<>();

        memtableLock.readLock().lock();
        iterators.add(memtable.iterator(from, to));
        iterators.add(immtable.iterator(from, to));
        for (Resource level : tables.getLevels()) iterators.add(level.iterator(from, to));
        memtableLock.readLock().unlock();

        return iterators;
    }

    @Override
    public void close() {
        memtableLock.writeLock().lock();
        createNewMemtable();
        tables.close();
        memtableLock.writeLock().unlock();
    }

    private void createNewMemtable() {
        tables.flushMemtable(memtable);
        immtable = memtable;
        memtable = new Memtable();
    }
}
