package com.accakyra.lsss.lsm.data.persistent;

import com.accakyra.lsss.lsm.data.memory.Memtable;
import com.accakyra.lsss.lsm.data.persistent.sst.Index;
import com.accakyra.lsss.lsm.data.persistent.sst.Table;
import com.accakyra.lsss.lsm.io.write.TableWriter;

import java.io.Closeable;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Levels implements Closeable {

    private final Map<Integer, Level> levels;
    private final TableManager tableManager;
    private final ReentrantReadWriteLock tablesLock;
    private final Metadata metadata;
    private final TableWriter tableWriter;
    private final Path folderName;

    public Levels(File data) {
        this.tableManager = new TableManager();
        this.levels = tableManager.readLevels(data);
        this.tablesLock = new ReentrantReadWriteLock();
        this.metadata = new Metadata(data);
        this.tableWriter = new TableWriter();
        this.folderName = data.toPath();
        if (levels.isEmpty()) levels.put(0, new Level());
    }

    public List<Level> getLevels() {
        tablesLock.readLock().lock();
        List<Level> levelsList = new ArrayList<>(levels.values());
        tablesLock.readLock().unlock();
        return levelsList;
    }

    public void flushMemtable(Memtable memtable) {
        Table table = tableManager.convertMemtableToTable(memtable);
        Index index = new Index(0, table.getKeysInfo());
        tableWriter.flushMemtable(table, metadata.getAndIncrementTableId(),
                folderName, levels.get(0), index, tablesLock);
    }

    @Override
    public void close() {
        metadata.close();
        tableWriter.close();
    }
}
