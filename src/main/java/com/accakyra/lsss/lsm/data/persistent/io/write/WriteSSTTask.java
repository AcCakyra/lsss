package com.accakyra.lsss.lsm.data.persistent.io.write;

import com.accakyra.lsss.lsm.data.Run;
import com.accakyra.lsss.lsm.data.memory.Memtable;
import com.accakyra.lsss.lsm.data.persistent.io.Table;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

public class WriteSSTTask implements Runnable {

    private final Table table;
    private final SST sst;
    private final Path storage;
    private final Run level;
    private final Map<Integer, Memtable> immtables;
    private final Lock lock;
    private final Semaphore semaphore;

    public WriteSSTTask(Table table, SST sst, Path storage,
                        Run level, Map<Integer, Memtable> immtables,
                        Lock lock, Semaphore semaphore) {
        this.table = table;
        this.sst = sst;
        this.storage = storage;
        this.level = level;
        this.immtables = immtables;
        this.lock = lock;
        this.semaphore = semaphore;
    }

    @Override
    public void run() {
        TableWriter.writeTable(table, storage);

        lock.lock();
        level.add(sst);
        immtables.remove(table.getId());
        lock.unlock();
        semaphore.release();
    }
}
