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
import com.accakyra.lsss.lsm.data.persistent.sst.Index;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;
import com.accakyra.lsss.lsm.io.FileRemover;
import com.accakyra.lsss.lsm.util.iterators.IteratorsUtil;
import com.google.common.collect.Iterators;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Store implements Closeable {

    private class WriteSSTTask implements Runnable {

        private final Table table;
        private final SST sst;

        public WriteSSTTask(Table table, SST sst) {
            this.table = table;
            this.sst = sst;
        }

        @Override
        public void run() {
            TableWriter.writeTable(table, storage);
            lock.writeLock().lock();
            levels.addSST(0, sst);
            levels.deleteImmtable(table.getId());
            if (levels.levelOverflow(0) > 0) {
                compact();
            }
            lock.writeLock().unlock();
            semaphore.release();
        }
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
                        .map(SST::getIndex)
                        .map(Index::firstKey)
                        .min(ByteBuffer::compareTo)
                        .get();

                to = currentLevel.getSstables()
                        .stream()
                        .map(SST::getIndex)
                        .map(Index::lastKey)
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
                        from = sstToRemove.getIndex().firstKey();
                        to = sstToRemove.getIndex().lastKey();
                    } else {
                        if (from.compareTo(sstToRemove.getIndex().firstKey()) > 0) {
                            from = sstToRemove.getIndex().firstKey();
                        }
                        if (to.compareTo(sstToRemove.getIndex().lastKey()) < 0) {
                            to = sstToRemove.getIndex().lastKey();
                        }
                    }
                    sstablesToRemove.add(sstToRemove);
                    currentLevel.getSstables().remove(sstToRemove);
                }

                currentLevelIterator =
                        Iterators.concat(
                                sstablesToRemove
                                        .stream()
                                        .map(SST::iterator)
                                        .collect(Collectors.toList())
                                        .iterator());
            }

            levels.addLevel(level + 1);

            Level nextLevel = levels.getLevel(level + 1);
            Iterator<SST> sstIterator = nextLevel.getSstables().iterator();
            List<SST> nextLevelSstables = new ArrayList<>();

            while (sstIterator.hasNext()) {
                SST s = sstIterator.next();
                ByteBuffer first = s.getIndex().keys().first();
                ByteBuffer last = s.getIndex().keys().last();
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
                    IteratorsUtil.removeTombstonesIterator(
                            IteratorsUtil.distinctIterator(
                                    IteratorsUtil.mergeIterator(
                                            List.of(currentLevelIterator, nextLevelIterator)
                                    )));

            List<Memtable> memtables = new ArrayList<>();
            Memtable toFlush = new Memtable();
            while (iterator.hasNext()) {
                Record record = iterator.next();
                toFlush.upsert(record.getKey(), record.getValue());
                if (!toFlush.hasSpace()) {
                    memtables.add(toFlush);
                    toFlush = new Memtable();
                }
            }
            if (!toFlush.isEmpty()) memtables.add(toFlush);

            for (Memtable mem : memtables) {
                int id = metadata.getAndIncrementTableId();
                Table tableToFlush = TableConverter.convertMemtableToTable(mem, id);
                TableWriter.writeTable(tableToFlush, storage);
                SST s = TableConverter.convertMemtableToSST(mem, id, level + 1, storage);
                nextLevel.add(s);
            }
            for (SST sstToRemove : sstablesToRemove) {
                FileRemover.remove(sstToRemove.getName());
            }
            level++;
        }
    }

    private final Levels levels;
    private final ExecutorService writer;
    private final Semaphore semaphore;
    private final ReentrantReadWriteLock lock;
    private final Path storage;
    private final Metadata metadata;

    public Store(File data) {
        this.levels = new Levels(data);
        this.writer = Executors.newSingleThreadExecutor();
        this.metadata = new Metadata(data);
        this.semaphore = new Semaphore(Config.MAX_MEMTABLE_COUNT);
        this.lock = new ReentrantReadWriteLock();
        this.storage = data.toPath();
    }

    public List<Resource> getResources() {
        lock.readLock().lock();
        List<Resource> resources = levels.getResources();
        lock.readLock().unlock();
        return resources;
    }

    public void addMemtable(Memtable memtable) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        int tableId = metadata.getAndIncrementTableId();
        lock.writeLock().lock();
        levels.addImmtable(tableId, memtable);
        lock.writeLock().unlock();

        Table table = TableConverter.convertMemtableToTable(memtable, tableId);
        SST sst = TableConverter.convertMemtableToSST(memtable, tableId, 0, storage);
        writer.submit(new WriteSSTTask(table, sst));
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
