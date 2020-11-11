package com.accakyra.lsss.lsm.io.write;

import com.accakyra.lsss.lsm.data.persistent.sst.Index;
import com.accakyra.lsss.lsm.data.persistent.Level;
import com.accakyra.lsss.lsm.data.persistent.sst.Table;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TableWriter implements Closeable {

    private final ExecutorService writer;
    private Future<?> flushTaskResult;

    public TableWriter() {
        this.writer = Executors.newSingleThreadExecutor();
    }

    public void flushMemtable(Table table, int tableId, Path path, Level level, Index index,
                              ReentrantReadWriteLock lock) {
        waitUntilMemtableIsFlushed();
        WriteSSTableTask writeSSTableTask = new WriteSSTableTask(table, tableId, path, level, index, lock);
        flushTaskResult = writer.submit(writeSSTableTask);
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
