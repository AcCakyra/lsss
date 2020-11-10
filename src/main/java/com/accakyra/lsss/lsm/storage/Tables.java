package com.accakyra.lsss.lsm.storage;

import com.accakyra.lsss.lsm.Metadata;
import com.accakyra.lsss.lsm.TableManager;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Tables implements Closeable {

    private final Map<Integer, Level> levels;
    private final ReentrantReadWriteLock tablesLock;
    private final TableManager tableManager;
    private final Metadata metadata;

    public Tables(File data) {
        this.tableManager = new TableManager(data);
        this.metadata = new Metadata(data);
        this.levels = tableManager.readLevels();
        this.tablesLock = new ReentrantReadWriteLock();
        if (levels.isEmpty()) {
            levels.put(0, new Level());
        }
    }

    public List<Level> getLevels() {
        tablesLock.readLock().lock();
        List<Level> levelsList = new ArrayList<>(levels.values());
        tablesLock.readLock().unlock();
        return levelsList;
    }

    public void flushMemtable(Memtable memtable) {
        tableManager.flush(levels.get(0), tablesLock, memtable, metadata.getAndIncrementTableId());
    }

    @Override
    public void close() {
        metadata.close();
        tableManager.close();
    }
}
