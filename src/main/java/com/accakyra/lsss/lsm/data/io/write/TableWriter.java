package com.accakyra.lsss.lsm.data.io.write;

import com.accakyra.lsss.lsm.data.persistent.Level;
import com.accakyra.lsss.lsm.data.persistent.sst.SST;
import com.accakyra.lsss.lsm.data.io.Table;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

public class TableWriter implements Closeable {

    private final ExecutorService writer;
    private Future<?> flushTaskResult;

    public TableWriter() {
        this.writer = Executors.newSingleThreadExecutor();
    }

    public void flushTable(Table table, Path storagePath, SST sst, Lock lock, Level level) {
        waitUntilMemtableIsFlushed();
        AddSSTTask addSSTTask = new AddSSTTask(table, storagePath, sst, lock, level);
        flushTaskResult = writer.submit(addSSTTask);
    }

    private void waitUntilMemtableIsFlushed() {
        try {
            if (flushTaskResult != null) {
                flushTaskResult.get();
                flushTaskResult = null;
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            writer.shutdown();
            writer.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
