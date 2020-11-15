package com.accakyra.lsss.lsm.data.persistent;

import com.accakyra.lsss.lsm.data.TableConverter;
import com.accakyra.lsss.lsm.data.memory.Memtable;
import com.accakyra.lsss.lsm.data.persistent.io.Table;
import com.accakyra.lsss.lsm.data.persistent.io.write.TableWriter;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

public class WriteSSTTask implements Runnable {

    private final Memtable memtable;
    private final int tableId;
    private final Path storage;
    private final Level level;
    private final Map<Integer, Memtable> immtables;
    private final Lock lock;
    private Semaphore semaphore;

    public WriteSSTTask(Memtable memtable, int tableId,
                        Path storage, Level level,
                        Map<Integer, Memtable> immtables,
                        Lock lock, Semaphore semaphore) {
        this.memtable = memtable;
        this.tableId = tableId;
        this.storage = storage;
        this.level = level;
        this.immtables = immtables;
        this.lock = lock;
        this.semaphore = semaphore;
    }

    @Override
    public void run() {
        Table table = TableConverter.convertMemtableToTable(memtable, tableId);
        TableWriter.writeTable(table, storage);
        SST sst = TableConverter.convertMemtableToSST(memtable, tableId, storage);

        lock.lock();
        level.add(sst);
        immtables.remove(tableId);
        lock.unlock();

        semaphore.release();
    }
}
