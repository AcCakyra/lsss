package com.accakyra.lsss.lsm.io;

import com.accakyra.lsss.lsm.store.MetaData;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

public class TableWriter {

    private final ExecutorService writer;
    private final File data;
    private final MetaData metaData;
    private Future<?> flushTaskResult;

    public TableWriter(MetaData metaData, File data) {
        this.data = data;
        this.metaData = metaData;
        this.writer = Executors.newSingleThreadExecutor();
    }

    public void scheduleMemtableFlushing(ByteBuffer sstBuffer, ByteBuffer indexBuffer) {
        waitUntilMemtableIsFlushed();
        WriteSSTableTask writeSSTableTask = new WriteSSTableTask(sstBuffer, indexBuffer, data.toPath(), metaData);
        flushTaskResult = writer.submit(writeSSTableTask);
    }

    public void close() {
        try {
            writer.shutdown();
            writer.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
}
