package com.accakyra.lsss.lsm.data.persistent;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.Config;
import com.accakyra.lsss.lsm.data.Resource;
import com.accakyra.lsss.lsm.data.TableConverter;
import com.accakyra.lsss.lsm.data.memory.Memtable;
import com.accakyra.lsss.lsm.data.persistent.io.Table;
import com.accakyra.lsss.lsm.data.persistent.io.read.TableReader;
import com.accakyra.lsss.lsm.data.persistent.io.write.TableWriter;
import com.accakyra.lsss.lsm.data.persistent.level.Level;
import com.accakyra.lsss.lsm.data.persistent.level.Level0;
import com.accakyra.lsss.lsm.data.persistent.level.LevelN;
import com.accakyra.lsss.lsm.data.persistent.level.Levels;
import com.accakyra.lsss.lsm.data.persistent.sst.KeyInfo;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;
import com.accakyra.lsss.lsm.util.FileNameUtil;
import com.accakyra.lsss.lsm.util.iterators.IteratorsUtil;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Uninterruptibles;
import lombok.extern.java.Log;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@Log
public class Store implements Closeable {

    private class PutMemtableTask implements Runnable {

        private final Memtable immtable;
        private final int tableId;

        public PutMemtableTask(Memtable immtable, int tableId) {
            this.immtable = immtable;
            this.tableId = tableId;
        }

        @Override
        public void run() {
            SST sst = flushMemtable(immtable, tableId, 0);
            levelsLock.writeLock().lock();
            Level newZeroLevel = levels.getLevel(0);
            newZeroLevel.add(sst);
            levels.addLevel(0, newZeroLevel);
            levels.deleteImmtable(sst.getId());
            semaphore.release();
            if (newZeroLevel.size() > Math.pow(config.getFanout(), 1)) {
                compact();
            }
            levelsLock.writeLock().unlock();
        }
    }

    private class RemoveFilesTask implements Runnable {

        private final List<SST> sstablesToRemove;

        public RemoveFilesTask(List<SST> sstablesToRemove) {
            this.sstablesToRemove = sstablesToRemove;
        }

        @Override
        public void run() {
            fileLock.writeLock().lock();
            for (SST sst : sstablesToRemove) {
                int id = sst.getId();
                try {
                    Files.deleteIfExists(FileNameUtil.buildIndexFileName(storage, id));
                    Files.deleteIfExists(FileNameUtil.buildSSTableFileName(storage, id));
                } catch (IOException e) {
                    log.log(java.util.logging.Level.SEVERE,
                            "Cannot delete sst and index with id : " + id + " from disk", e);
                    throw new RuntimeException(e);
                }
            }
            fileLock.writeLock().unlock();
        }
    }

    private final Levels levels;
    private final ExecutorService memtablePusher;
    private final ExecutorService fileWorker;
    private final Semaphore semaphore;
    private final ReadWriteLock fileLock;
    private final ReadWriteLock levelsLock;
    private final Path storage;
    private final Metadata metadata;
    private final Config config;

    public Store(File data, ReadWriteLock fileLock, Config config) {
        this.levels = new Levels(TableReader.readLevels(data, config));
        this.memtablePusher = Executors.newSingleThreadExecutor();
        this.fileWorker = Executors.newSingleThreadExecutor();
        this.metadata = new Metadata(data);
        this.semaphore = new Semaphore(config.getMaxImmtableCount());
        this.fileLock = fileLock;
        this.levelsLock = new ReentrantReadWriteLock();
        this.storage = data.toPath();
        this.config = config;
    }

    public Record get(ByteBuffer key) {
        levelsLock.readLock().lock();
        Record record = levels.get(key);
        levelsLock.readLock().unlock();
        return record;
    }

    public List<Resource> getResources() {
        levelsLock.readLock().lock();
        List<Resource> resources = levels.getResources();
        levelsLock.readLock().unlock();
        return resources;
    }

    public void addMemtable(Memtable memtable) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        int tableId = metadata.getAndIncrementTableId();
        levelsLock.writeLock().lock();
        levels.addImmtable(tableId, memtable);
        levelsLock.writeLock().unlock();

        memtablePusher.submit(new PutMemtableTask(memtable, tableId));
    }

    private SST flushMemtable(Memtable immtable, int tableId, int level) {
        Table table = TableConverter.convertMemtableToTable(immtable, level);
        try {
            fileWorker.submit(() -> TableWriter.writeTable(table, tableId, storage)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        NavigableMap<ByteBuffer, KeyInfo> index =
                TableConverter.parseIndexBufferSparse(
                        table.getIndexBuffer().position(4), config.getMaxKeySize(), config.getSparseStep());
        Path sstFileName = FileNameUtil.buildSSTableFileName(storage, tableId);
        Path indexFileName = FileNameUtil.buildIndexFileName(storage, tableId);
        return new SST(index, tableId, sstFileName, indexFileName, level);
    }

    private void compact() {
        int level = 0;
        while (levels.getLevel(level).size() - Math.pow(config.getFanout(), level + 1) > 0) {
            List<SST> sstablesToRemove = new ArrayList<>();
            Iterator<Record> currentLevelIterator;
            ByteBuffer from = null;
            ByteBuffer to = null;

            Level currentLevel = levels.getLevel(level);

            if (level == 0) {
                from = currentLevel.getSstables()
                        .stream()
                        .map(SST::firstKey)
                        .min(ByteBuffer::compareTo)
                        .get();

                to = currentLevel.getSstables()
                        .stream()
                        .map(SST::lastKey)
                        .max(ByteBuffer::compareTo)
                        .get();

                sstablesToRemove.addAll(currentLevel.getSstables());

                currentLevelIterator =
                        IteratorsUtil.distinctIterator(
                                IteratorsUtil.mergeIterator(
                                        currentLevel.getSstables()
                                                .stream()
                                                .map(SST::iterator)
                                                .collect(Collectors.toList())));
                levels.addLevel(0, new Level0());
            } else {
                int levelOverflow = (int) (levels.getLevel(level).size() - Math.pow(config.getFanout(), level + 1));

                for (int i = 0; i < levelOverflow; i++) {
                    int random = new Random().nextInt(currentLevel.size());
                    SST sstToRemove = currentLevel.getSstables().stream().skip(random).findFirst().get();
                    if (from == null) {
                        from = sstToRemove.firstKey();
                        to = sstToRemove.lastKey();
                    } else {
                        if (from.compareTo(sstToRemove.firstKey()) > 0) {
                            from = sstToRemove.firstKey();
                        }
                        if (to.compareTo(sstToRemove.lastKey()) < 0) {
                            to = sstToRemove.lastKey();
                        }
                    }
                    sstablesToRemove.add(sstToRemove);
                    currentLevel.getSstables().remove(sstToRemove);
                }

                sstablesToRemove.sort(Comparator.comparing(SST::firstKey));
                currentLevelIterator = Iterators.concat(sstablesToRemove
                        .stream()
                        .map(SST::iterator)
                        .collect(Collectors.toList()).iterator()
                );
            }

            if (levels.getLevel(level + 1) == null) {
                levels.addLevel(level + 1, new LevelN());
            }

            Level nextLevel = levels.getLevel(level + 1);
            Iterator<SST> sstIterator = nextLevel.getSstables().iterator();
            List<SST> nextLevelSstables = new ArrayList<>();

            while (sstIterator.hasNext()) {
                SST s = sstIterator.next();
                ByteBuffer first = s.firstKey();
                ByteBuffer last = s.lastKey();
                if (last.compareTo(from) >= 0 && first.compareTo(to) <= 0) {
                    nextLevelSstables.add(s);
                    sstablesToRemove.add(s);
                    sstIterator.remove();
                }
            }

            Iterator<Record> nextLevelIterator =
                    Iterators.concat(nextLevelSstables
                            .stream()
                            .map(SST::iterator)
                            .collect(Collectors.toList())
                            .iterator());

            Iterator<Record> iterator =
                    IteratorsUtil.distinctIterator(
                            IteratorsUtil.mergeIterator(
                                    List.of(currentLevelIterator, nextLevelIterator)));


            Level newNextLevel = nextLevel.copy();
            Memtable memtable = new Memtable();

            while (iterator.hasNext()) {
                Record record = iterator.next();
                memtable.upsert(record.getKey(), record.getValue());
                if (memtable.getSize() > config.getMemtableSize() || !iterator.hasNext()) {
                    int tableId = metadata.getAndIncrementTableId();
                    SST sst = flushMemtable(memtable, tableId, level + 1);
                    newNextLevel.add(sst);
                    memtable = new Memtable();
                }
            }

            levels.addLevel(level + 1, newNextLevel);
            fileWorker.execute(new RemoveFilesTask(sstablesToRemove));
            level++;
        }
    }

    @Override
    public void close() {
        try {
            for (int i = 0; i < config.getMaxImmtableCount(); i++) semaphore.acquire();

            memtablePusher.shutdown();
            memtablePusher.awaitTermination(1, TimeUnit.HOURS);
            fileWorker.shutdown();
            fileWorker.awaitTermination(1, TimeUnit.HOURS);

            metadata.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
