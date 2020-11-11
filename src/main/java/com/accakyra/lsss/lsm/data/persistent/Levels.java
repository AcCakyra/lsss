package com.accakyra.lsss.lsm.data.persistent;

import com.accakyra.lsss.lsm.data.TableConverter;
import com.accakyra.lsss.lsm.data.io.Table;
import com.accakyra.lsss.lsm.data.io.read.TableReader;
import com.accakyra.lsss.lsm.data.io.write.TableWriter;
import com.accakyra.lsss.lsm.data.memory.Memtable;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;

import java.io.Closeable;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Levels implements Closeable {

    private final Map<Integer, Level> levels;
    private final ReentrantReadWriteLock levelsLock;
    private final Metadata metadata;
    private final TableWriter tableWriter;
    private final TableReader tableReader;
    private final Path storagePath;

    public Levels(File data) {
        this.tableWriter = new TableWriter();
        this.tableReader = new TableReader();
        this.levelsLock = new ReentrantReadWriteLock();
        this.metadata = new Metadata(data);
        this.storagePath = data.toPath();
        this.levels = tableReader.readLevels(data);
    }

    public List<Level> getLevels() {
        levelsLock.readLock().lock();
        List<Level> levelsList = new ArrayList<>(levels.values());
        levelsLock.readLock().unlock();
        return levelsList;
    }

    public void writeMemtable(Memtable memtable) {
        int tableId = metadata.getAndIncrementTableId();
        SST sst = TableConverter.convertMemtableToSST(memtable, tableId, storagePath);
        Table table = TableConverter.convertMemtableToTable(memtable, tableId);
        tableWriter.flushTable(table, storagePath, sst, levelsLock.writeLock(), levels.get(0));
    }

    @Override
    public void close() {
        metadata.close();
        tableWriter.close();
    }
}
