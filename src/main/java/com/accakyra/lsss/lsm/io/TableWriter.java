package com.accakyra.lsss.lsm.io;

import com.accakyra.lsss.lsm.LSMProperties;
import com.accakyra.lsss.lsm.store.Key;
import com.accakyra.lsss.lsm.store.MemTable;
import com.accakyra.lsss.lsm.store.MetaData;
import com.accakyra.lsss.lsm.store.SST;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
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

    public void scheduleFlushing(MemTable memtable) {
        waitUntilTableIsFlushed();
        SST sstable = convertMemTableToSSTable(memtable);
        WriteSSTableTask writeSSTableTask = new WriteSSTableTask(sstable, data.toPath(), metaData);
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

    private void waitUntilTableIsFlushed() {
        try {
            if (flushTaskResult != null) {
                flushTaskResult.get();
                flushTaskResult = null;
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private SST convertMemTableToSSTable(MemTable memTable) {
        int indexBufferSize = memTable.calcKeysSize();
        ByteBuffer indexBuffer = ByteBuffer.allocate(indexBufferSize);
        ByteBuffer dataBuffer = ByteBuffer.allocate(LSMProperties.MEMTABLE_THRESHOLD);
        int valueOffset = 0;

        Iterator<Map.Entry<Key, ByteBuffer>> memtableIterator = memTable.getIterator();
        while (memtableIterator.hasNext()) {
            Map.Entry<Key, ByteBuffer> entry = memtableIterator.next();
            byte[] key = entry.getKey().getKey().array();
            byte[] value = entry.getValue().array();
            dataBuffer.put(key);
            dataBuffer.put(value);

            indexBuffer.putInt(key.length);
            indexBuffer.put(key);

            valueOffset += key.length;
            indexBuffer.putInt(valueOffset);
            indexBuffer.putInt(value.length);
            valueOffset += value.length;
        }

        dataBuffer.flip();
        indexBuffer.flip();
        return new SST(dataBuffer, indexBuffer);
    }
}
