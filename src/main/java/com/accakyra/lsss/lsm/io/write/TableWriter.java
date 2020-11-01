package com.accakyra.lsss.lsm.io.write;

import com.accakyra.lsss.lsm.Metadata;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.*;

public class TableWriter implements Closeable {

    private final ExecutorService writer;
    private Future<?> flushTaskResult;

    public TableWriter() {
        this.writer = Executors.newSingleThreadExecutor();
    }

    public void writeTable(ByteBuffer sstBuffer, ByteBuffer indexBuffer, Metadata metadata, Path path) {
        waitUntilMemtableIsFlushed();
        WriteSSTableTask writeSSTableTask = new WriteSSTableTask(sstBuffer, indexBuffer, path, metadata);
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
