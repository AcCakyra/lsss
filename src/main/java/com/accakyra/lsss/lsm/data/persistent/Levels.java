package com.accakyra.lsss.lsm.data.persistent;

import com.accakyra.lsss.lsm.Config;
import com.accakyra.lsss.lsm.data.Resource;
import com.accakyra.lsss.lsm.data.memory.Memtable;
import com.accakyra.lsss.lsm.data.persistent.io.read.TableReader;

import java.io.Closeable;
import java.io.File;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Levels implements Closeable {

    private final Map<Integer, Level> levels;
    private final ReentrantReadWriteLock levelsLock;
    private final Metadata metadata;
    private final TableReader tableReader;
    private final Path storagePath;
    private final TreeMap<Integer, Memtable> immtables;
    private final ExecutorService writer;
    private final Semaphore semaphore;

    public Levels(File data) {
        this.tableReader = new TableReader();
        this.levelsLock = new ReentrantReadWriteLock();
        this.metadata = new Metadata(data);
        this.storagePath = data.toPath();
        this.levels = tableReader.readLevels(data);
        this.immtables = new TreeMap<>(Comparator.reverseOrder());
        this.writer = Executors.newSingleThreadExecutor();
        this.semaphore = new Semaphore(Config.MAX_MEMTABLE_COUNT);
    }

    public List<Resource> getAllResources() {
        List<Resource> levelsList = new ArrayList<>();
        levelsLock.readLock().lock();
        levelsList.addAll(immtables.values());
        levelsList.addAll(levels.values());
        levelsLock.readLock().unlock();
        return levelsList;
    }

    public void putResource(Memtable memtable) {
        try {
            semaphore.acquire();
            int tableId = metadata.getAndIncrementTableId();
            levelsLock.writeLock().lock();
            immtables.put(tableId, memtable);
            levelsLock.writeLock().unlock();
            writer.execute(new WriteSSTTask(memtable, tableId,
                    storagePath, levels.get(0),
                    immtables, levelsLock.writeLock(), semaphore));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            for (int i = 0; i < Config.MAX_MEMTABLE_COUNT; i++) {
                semaphore.acquire();
            }
            writer.shutdown();
            writer.awaitTermination(1, TimeUnit.HOURS);
            metadata.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
