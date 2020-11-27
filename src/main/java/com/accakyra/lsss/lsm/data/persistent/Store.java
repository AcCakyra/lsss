package com.accakyra.lsss.lsm.data.persistent;

import com.accakyra.lsss.Record;
import com.accakyra.lsss.lsm.Config;
import com.accakyra.lsss.lsm.data.Resource;
import com.accakyra.lsss.lsm.data.TableConverter;
import com.accakyra.lsss.lsm.data.memory.Memtable;
import com.accakyra.lsss.lsm.data.persistent.io.Table;
import com.accakyra.lsss.lsm.data.persistent.io.write.TableWriter;
import com.accakyra.lsss.lsm.data.persistent.level.Level;
import com.accakyra.lsss.lsm.data.persistent.level.Levels;
import com.accakyra.lsss.lsm.data.persistent.sst.KeyInfo;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;
import com.accakyra.lsss.lsm.io.FileRemover;
import com.accakyra.lsss.lsm.util.FileNameUtil;
import com.accakyra.lsss.lsm.util.iterators.IteratorsUtil;
import com.google.common.collect.Iterators;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Store implements Closeable {

    private class WriteSSTTask implements Runnable {

        private final Memtable immtable;
        private final int tableId;

        public WriteSSTTask(Memtable immtable, int tableId) {
            this.immtable = immtable;
            this.tableId = tableId;
        }

        @Override
        public void run() {
            int level = 0;
            Table table = TableConverter.convertMemtableToTable(immtable, level);
            TableWriter.writeTable(table, tableId, storage);
            NavigableMap<ByteBuffer, KeyInfo> index = TableConverter.parseIndexBuffer(table.getIndexBuffer().position(4), true);
            Path sstFileName = FileNameUtil.buildSSTableFileName(storage, tableId);
            Path indexFileName = FileNameUtil.buildIndexFileName(storage, tableId);
            SST sst = new SST(index, tableId, sstFileName, indexFileName, level);
            compactor.execute(new CompactTask(sst));
        }
    }

    private class CompactTask implements Runnable {

        private final SST sst;

        public CompactTask(SST sst) {
            this.sst = sst;
        }

        @Override
        public void run() {
            levelsLock.writeLock().lock();
            levels.addSST(0, sst);
            levels.deleteImmtable(sst.getId());
            semaphore.release();
            if (levels.levelOverflow(0) > 0) {
                compact();
            }
            levelsLock.writeLock().unlock();
        }
    }

    private final Levels levels;
    private final ExecutorService writer;
    private final ExecutorService compactor;
    private final ExecutorService fileRemover;
    private final Semaphore semaphore;
    private final ReadWriteLock fileLock;
    private final ReadWriteLock levelsLock;
    private final Path storage;
    private final Metadata metadata;

    public Store(File data, ReadWriteLock fileLock) {
        this.levels = new Levels(data);
        this.writer = Executors.newSingleThreadExecutor();
        this.compactor = Executors.newSingleThreadExecutor();
        this.fileRemover = Executors.newSingleThreadExecutor();
        this.metadata = new Metadata(data);
        this.semaphore = new Semaphore(Config.MAX_MEMTABLE_COUNT);
        this.fileLock = fileLock;
        this.levelsLock = new ReentrantReadWriteLock();
        this.storage = data.toPath();
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
            e.printStackTrace();
        }

        int tableId = metadata.getAndIncrementTableId();
        levelsLock.writeLock().lock();
        levels.addImmtable(tableId, memtable);
        levelsLock.writeLock().unlock();

        writer.submit(new WriteSSTTask(memtable, tableId));
    }

    private void compact() {
        int level = 0;
        while (levels.levelOverflow(level) > 0) {
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
                levels.resetZeroLevel();
            } else {
                int diff = levels.levelOverflow(level);

                for (int i = 0; i < diff; i++) {
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

            levels.addLevel(level + 1);
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

            List<Memtable> memtables = new ArrayList<>();
            Memtable memtable = new Memtable();

            while (iterator.hasNext()) {
                Record record = iterator.next();
                memtable.upsert(record.getKey(), record.getValue());
                if (!memtable.hasSpace()) {
                    memtables.add(memtable);
                    memtable = new Memtable();
                }
            }
            if (!memtable.isEmpty()) memtables.add(memtable);

            for (Memtable mem : memtables) {
                int tableId = metadata.getAndIncrementTableId();
                Table table = TableConverter.convertMemtableToTable(mem, level + 1);
                TableWriter.writeTable(table, tableId, storage);
                NavigableMap<ByteBuffer, KeyInfo> index = TableConverter.parseIndexBuffer(table.getIndexBuffer().position(4), true);
                Path sstFileName = FileNameUtil.buildSSTableFileName(storage, tableId);
                Path indexFileName = FileNameUtil.buildIndexFileName(storage, tableId);
                SST sst = new SST(index, tableId, sstFileName, indexFileName, level + 1);
                nextLevel.add(sst);
            }

            // todo: move to another thread
            // coz we cannot complete compaction if someone has closable iterator
            fileLock.writeLock().lock();
            for (SST sst : sstablesToRemove) {
                int id = sst.getId();
                FileRemover.remove(FileNameUtil.buildIndexFileName(storage, id));
                FileRemover.remove(FileNameUtil.buildSSTableFileName(storage, id));
            }
            fileLock.writeLock().unlock();

            level++;
        }
    }

    @Override
    public void close() {
        try {
            for (int i = 0; i < Config.MAX_MEMTABLE_COUNT; i++) {
                semaphore.acquire();
            }

            fileRemover.shutdown();
            compactor.shutdown();
            writer.shutdown();

            fileRemover.awaitTermination(1, TimeUnit.HOURS);
            compactor.awaitTermination(1, TimeUnit.HOURS);
            writer.awaitTermination(1, TimeUnit.HOURS);

            metadata.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
