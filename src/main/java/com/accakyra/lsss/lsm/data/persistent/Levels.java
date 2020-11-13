package com.accakyra.lsss.lsm.data.persistent;

import com.accakyra.lsss.lsm.Config;
import com.accakyra.lsss.lsm.data.Resource;
import com.accakyra.lsss.lsm.data.TableConverter;
import com.accakyra.lsss.lsm.data.persistent.io.Table;
import com.accakyra.lsss.lsm.data.persistent.io.read.TableReader;
import com.accakyra.lsss.lsm.data.persistent.io.write.TableWriter;
import com.accakyra.lsss.lsm.data.memory.Memtable;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;

import java.io.Closeable;
import java.io.File;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Levels implements Closeable {

    private final Map<Integer, Level> levels;
    private final ReentrantReadWriteLock levelsLock;
    private final Metadata metadata;
    private final TableWriter tableWriter;
    private final TableReader tableReader;
    private final Path storagePath;
    private final BlockingQueue<Integer> immtablesId;
    private final Map<Integer, Memtable> immtables;
    private final ExecutorService writer;
    private volatile boolean workerFlag;
    private Future<?> workerResult;

    public Levels(File data) {
        this.tableWriter = new TableWriter();
        this.tableReader = new TableReader();
        this.levelsLock = new ReentrantReadWriteLock();
        this.metadata = new Metadata(data);
        this.storagePath = data.toPath();
        this.levels = tableReader.readLevels(data);
        this.immtablesId = new LinkedBlockingQueue<>(Config.MAX_MEMTABLE_COUNT);
        this.immtables = new ConcurrentHashMap<>();
        this.workerFlag = true;
        this.writer = Executors.newSingleThreadExecutor();

        workerResult = writer.submit(() -> {
            while (workerFlag || immtablesId.remainingCapacity() < Config.MAX_MEMTABLE_COUNT) {
                try {
                    int tableId = immtablesId.take();
                    Memtable memtable = immtables.get(tableId);

                    Table table = TableConverter.convertMemtableToTable(memtable, tableId);
                    tableWriter.flushTable(table, storagePath);

                    SST sst = TableConverter.convertMemtableToSST(memtable, tableId, storagePath);
                    levelsLock.writeLock().lock();
                    levels.get(0).add(sst);
                    immtables.remove(tableId);
                    levelsLock.writeLock().unlock();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public List<Resource> getLevels() {
        List<Resource> levelsList = new ArrayList<>(immtables.values());
        levelsLock.readLock().lock();
        levelsList.addAll(levels.values());
        levelsLock.readLock().unlock();
        return levelsList;
    }

    public void writeMemtable(Memtable memtable) {
        int tableId = metadata.getAndIncrementTableId();
        immtables.put(tableId, memtable);

        try {
            immtablesId.put(tableId);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        shutdownWriter();
        metadata.close();
    }

    private void shutdownWriter() {
        try {
            workerFlag = false;
            workerResult.get();
            writer.shutdown();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
