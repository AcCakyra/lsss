package com.accakyra.lsss.lsm;

import com.accakyra.lsss.DAO;
import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.data.memory.Memtable;
import com.accakyra.lsss.lsm.data.Resource;
import com.accakyra.lsss.lsm.data.persistent.Levels;

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
    private final Levels levels;
    private final ReadWriteLock lock;

    public LSMTree(File data) {
        memtable = new Memtable();
        levels = new Levels(data);
        lock = new ReentrantReadWriteLock();
    }

    @Override
    public void upsert(ByteBuffer key, ByteBuffer value) {
        lock.writeLock().lock();
        memtable.upsert(key, value);
        if (!memtable.hasSpace()) {
            writeMemtable();
        }
        lock.writeLock().unlock();
    }

    @Override
    public ByteBuffer get(ByteBuffer key) throws NoSuchElementException {
        lock.readLock().lock();
        Record record = memtable.get(key);
        if (record == null) {
            for (Resource resource : levels.getAllResources()) {
                record = resource.get(key);
                if (record != null) break;
            }
        }
        lock.readLock().unlock();
        return extractValue(record);
    }

    @Override
    public void delete(ByteBuffer key) {
        upsert(key, TOMBSTONE);
    }

    @Override
    public Iterator<Record> iterator() {
        List<Iterator<Record>> iterators = new ArrayList<>();
        lock.readLock().lock();
        iterators.add(memtable.iterator());
        for (Resource resource : levels.getAllResources()) iterators.add(resource.iterator());
        lock.readLock().unlock();
        return new DeletIterator(new MergeIterator(iterators));
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        lock.readLock().lock();
        iterators.add(memtable.iterator(from));
        for (Resource resource : levels.getAllResources()) iterators.add(resource.iterator(from));
        lock.readLock().unlock();
        return new DeletIterator(new MergeIterator(iterators));
    }

    @Override
    public Iterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        List<Iterator<Record>> iterators = new ArrayList<>();
        lock.readLock().lock();
        iterators.add(memtable.iterator(from, to));
        for (Resource resource : levels.getAllResources()) iterators.add(resource.iterator(from, to));
        lock.readLock().unlock();
        return new DeletIterator(new MergeIterator(iterators));
    }

    private ByteBuffer extractValue(Record record) {
        if (record == null || record.getValue().equals(TOMBSTONE)) throw new NoSuchElementException();
        else return record.getValue();
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        if (!memtable.isEmpty()) writeMemtable();
        levels.close();
        lock.writeLock().unlock();
    }

    private void writeMemtable() {
        levels.putResource(memtable);
        memtable = new Memtable();
    }
}
