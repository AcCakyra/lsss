package com.accakyra.lsss.lsm;

import com.accakyra.lsss.CloseableIterator;
import com.accakyra.lsss.DAO;
import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.data.memory.Memtable;
import com.accakyra.lsss.lsm.data.Resource;
import com.accakyra.lsss.lsm.data.persistent.Store;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.accakyra.lsss.lsm.util.iterators.IteratorsUtil.*;

public class LSMTree implements DAO {

    private Memtable memtable;
    private final Store store;
    private final ReadWriteLock lsmLock;
    private final ReadWriteLock fileLock;

    public LSMTree(File data) {
        this.memtable = new Memtable();
        this.lsmLock = new ReentrantReadWriteLock();
        this.fileLock = new ReentrantReadWriteLock();
        this.store = new Store(data, fileLock);
    }

    @Override
    public void upsert(ByteBuffer key, ByteBuffer value) {
        lsmLock.writeLock().lock();
        memtable.upsert(key, value);
        if (!memtable.hasSpace()) {
            writeMemtable();
        }
        lsmLock.writeLock().unlock();
    }

    @Override
    public ByteBuffer get(ByteBuffer key) throws NoSuchElementException {
        lsmLock.readLock().lock();
        Record record = memtable.get(key);
        if (record == null) record = store.get(key);
        lsmLock.readLock().unlock();
        return extractValue(record);
    }

    @Override
    public void delete(ByteBuffer key) {
        upsert(key, Record.TOMBSTONE);
    }

    @Override
    public CloseableIterator<Record> iterator() {
        lsmLock.readLock().lock();
        fileLock.readLock().lock();

        List<Iterator<Record>> iterators = new ArrayList<>();
        iterators.add(memtable.iterator());
        for (Resource resource : store.getResources()) iterators.add(resource.iterator());
        lsmLock.readLock().unlock();

        return wrapIterators(iterators);
    }

    @Override
    public CloseableIterator<Record> iterator(ByteBuffer from) {
        lsmLock.readLock().lock();
        fileLock.readLock().lock();

        List<Iterator<Record>> iterators = new ArrayList<>();
        iterators.add(memtable.iterator(from));
        for (Resource resource : store.getResources()) iterators.add(resource.iterator(from));
        lsmLock.readLock().unlock();

        return wrapIterators(iterators);
    }

    @Override
    public CloseableIterator<Record> iterator(ByteBuffer from, ByteBuffer to) {
        lsmLock.readLock().lock();
        fileLock.readLock().lock();

        List<Iterator<Record>> iterators = new ArrayList<>();
        iterators.add(memtable.iterator(from, to));
        for (Resource resource : store.getResources()) iterators.add(resource.iterator(from, to));
        lsmLock.readLock().unlock();

        return wrapIterators(iterators);
    }

    private ByteBuffer extractValue(Record record) {
        if (record == null || record.getValue().equals(Record.TOMBSTONE)) throw new NoSuchElementException();
        else return record.getValue();
    }

    private CloseableIterator<Record> wrapIterators(List<Iterator<Record>> iterators) {
        return closeableIterator(
                removeTombstonesIterator(
                        distinctIterator(
                                mergeIterator(iterators))),
                () -> fileLock.readLock().unlock());
    }

    @Override
    public void close() {
        lsmLock.writeLock().lock();
        if (!memtable.isEmpty()) writeMemtable();
        store.close();
        lsmLock.writeLock().unlock();
    }

    private void writeMemtable() {
        store.addMemtable(memtable);
        memtable = new Memtable();
    }
}
