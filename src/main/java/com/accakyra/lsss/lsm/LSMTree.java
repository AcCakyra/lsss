package com.accakyra.lsss.lsm;

import com.accakyra.lsss.DAO;
import com.accakyra.lsss.lsm.io.TableReader;
import com.accakyra.lsss.lsm.io.TableWriter;
import com.accakyra.lsss.lsm.store.MemTable;
import com.accakyra.lsss.lsm.store.MetaData;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMTree implements DAO {

    private MemTable memtable;
    private MemTable immtable;
    private final TableWriter tableWriter;
    private final TableReader tableReader;
    private final ReadWriteLock mutex;
    private int memtableByteCount;
    private final MetaData metaData;

    public LSMTree(File data) {
        metaData = new MetaData(data);
        memtable = new MemTable();
        tableWriter = new TableWriter(metaData, data);
        tableReader = new TableReader(metaData, data);
        mutex = new ReentrantReadWriteLock();
    }

    public void upsert(ByteBuffer key, ByteBuffer value) {
        mutex.writeLock().lock();

        int newEntrySize = key.remaining() + value.remaining();
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
                value = tableReader.get(key);
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
        immtable = memtable;
        tableWriter.scheduleFlushing(immtable);
        memtable = new MemTable();
        memtableByteCount = 0;
    }
}
